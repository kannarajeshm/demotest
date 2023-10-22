#!/bin/bash

#
# Copyrighted as an unpublished work 2016 D&B.
# Proprietary and Confidential.  Use, possession and disclosure subject to license agreement.
# Unauthorized use, possession or disclosure is a violation of D&B's legal rights and may result
# in suit or prosecution.
#


# Remove all 30 days older HBASE snapshots and S3 backups directory


# Set up all the global variable we require
init() {
  local ARGUMENTS="$@"

  VERBOSE=

  # Parse option(s)
  if [ $# -ne 0 ]; then
    while [ "${1:0:1}" = "-" ] ; do
      if [ "$1" = "-h" -o "$1" = "--help" -o "$1" = "-?" ]; then
        helpPage
      else
        helpPage "Unexpected option '$1'"
      fi
      shift
    done
  fi

  if [ $# -ne 1 ]; then
    helpPage "Invalid argument: ${ARGUMENTS}"
  else
    S3PATH=$1/hbase
  fi
}


# Display a help page and exit the script. If, and only if, an error message is passed then return an error code
# $1...: Optional error messages
helpPage() {
  local RET

  if [ $# -eq 0 ]; then
    RET=0
  else
    RET=1
    if [ $# -ne 0 ]; then
      echo $1
      shift
    fi
    echo ""
  fi

  echo "Usage:"
  echo "  `basename $0` <s3-path>"
  echo
  echo "Delete 30days older snapshots from Hbase and S3"
  echo
  echo "Options are:"
  echo "   -h, -?, --help        Display this help page and exit."
  echo
  echo
  echo "Return Codes:"
  echo "   0 - Success"
  echo "   1 - Hadoop error"

  exit ${RET}
}


# Delete Snapshot

deleteSnapshot() {

  COMPAREDATE=$(date -d "30 days ago" "+%Y%m%d")
  
  echo -e "list_snapshots" | hbase shell -n > File.txt
  
  INDEX=`echo -e "list_snapshots" | hbase shell -n| grep -n 'seconds' | sed 's/^\([0-9]\+\):.*$/\1/'`
  FIRSTINDEX=$(expr "${INDEX}" + 1)
  LASTINDEX=`echo -e "list_snapshots" | hbase shell -n| wc -c`
  
  #echo -e "list_snapshots" | hbase shell -n | awk -F '-' '{ print $1"-"$2" "$3 }'| tail -n +6  2>/dev/null > SNAPSHOT.txt
  
  sed -n ''${FIRSTINDEX}','${LASTINDEX}' p' File.txt | awk -F '-' '{ print $1"-"$2" "$3 }'| tail -n +6  2>/dev/null > SNAPSHOT.txt

  local RET=$?

  if [ ${RET} -eq 0 ]; then
      
	  cat SNAPSHOT.txt | while read line
	  do
			SNAPSHOTNAME=`echo $line | awk -F ' ' '{ print $1 }'`
			SNAPSHOTDATE=`echo $line | awk -F ' ' '{ print $2 }'`
			if [[ $SNAPSHOTDATE != "" && $SNAPSHOTDATE -lt $COMPAREDATE ]]; then
				echo "Deleting Snapshot ${SNAPSHOTNAME}-${SNAPSHOTDATE}"
				echo -e "delete_snapshot '${SNAPSHOTNAME}-${SNAPSHOTDATE}'" | hbase shell -n 2>/dev/null
				echo "Successfully deleted Snapshot ${SNAPSHOTNAME}-${SNAPSHOTDATE}"
			fi
	  done
	  rm SNAPSHOT.txt
	  rm File.txt
  fi

}

deleteBackUpDirFromS3() {

	COMPAREDATE=$(date -d "30 days ago" "+%Y%m%d")

	hadoop fs -ls ${S3PATH} | tail -n +2 | awk -F ' ' '{print $6}' > DIRECTORY.txt
	
	local RET=$?
	
	if [ ${RET} -eq 0 ]; then
		cat DIRECTORY.txt | while read line
		do
			echo ${line} | awk -F '/' '{ print $5 }' >> DIRECTORYDATE.txt
		done
		cat DIRECTORYDATE.txt | while read var
		do
			DIRECTORY=${var}
			DATECHECK=`date "+%Y%m%d" -d ${var} > /dev/null  2>&1`
			IS_VALID=$?
			if [ ${IS_VALID} -eq 0  ]; then
				DATEFORMAT=`date -d ${var} "+%Y%m%d"`
				if [[ ${DATEFORMAT} -lt ${COMPAREDATE} ]]; then
					echo "Deleting Directory ${DIRECTORY}"
					hadoop fs -rm -r ${S3PATH}/${DIRECTORY}
					echo "Successfully deleted Directory ${DIRECTORY}"
				fi
			fi
		done
		rm DIRECTORY.txt
		rm DIRECTORYDATE.txt
	fi

}


## Script Entry Point ##

init $*
deleteSnapshot 2>&1
deleteBackUpDirFromS3 2>&1

exit 0