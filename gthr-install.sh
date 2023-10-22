#!/bin/bash

#
# Copyrighted as an unpublished work 2016 D&B.
# Proprietary and Confidential.  Use, possession and disclosure subject to license agreement.
# Unauthorized use, possession or disclosure is a violation of D&B's legal rights and may result
# in suit or prosecution.
#

#
# Global Trade Hadoop Repository installation script
#


#set -x

# Parse command line and set up environment variables
# Returns the function to carry out:
#   0 => install
#   1 => clean
#   2 => Help page
function init() {
  local FUNCTION=
  ARGUMENTS=
  SMALL_DATA=

  local INSTALL_SCRIPT=$( readlink -m $( type -p $0 )) # Full path to script
  export BASE_DIR=`dirname ${INSTALL_SCRIPT}`          # Directory script is run in
  export EXTRACT_DIR=${BASE_DIR}/gthr                  # Directory bundle is extracted into
  export INSTALL_DIR=${EXTRACT_DIR}/install            # Directory for installer scripts (as extracted from the zip)

  export APPLICATION_VERSION=@@VERSION@@

  BUNDLE=${BASE_DIR}/gthr-app-${APPLICATION_VERSION}.tar.gz
  INSTALL_DATA=${BASE_DIR}/gthr-install.dat
  OPTIONS=${BASE_DIR}/install.properties
  READ_ME=SETUP.txt

  if [ "${1^^}" = "--CLEAN" ]; then
    FUNCTION=1

    if [ $# -ne 1 ]; then
      helpPage "Unexpected arguments"
    fi
  elif [ "${1^^}" = "-H" -o "${1^^}" = "--HELP" -o "${1^^}" = "-?" ]; then
    FUNCTION=2
  else    # Install
    FUNCTION=0
    # Parse command line option(s)
    if [ $# -ne 0 ]; then
      while [ "${1:0:1}" = "-" ] ; do
        local OPTION=${1^^}

        if [ "${OPTION}" = "--OPTIONS" ] ; then
          shift
          if [ $# -eq 0 ] ; then
            helpPage "--options requires a file"
          else
            OPTIONS=$1
          fi
        elif [ "${OPTION}" = "--SMALL-DATA" ]; then
          SMALL_DATA=true
          ARGUMENTS="${ARGUMENTS} ${OPTION}"
        else
          helpPage "Unexpected option '$1'"
        fi
        shift
      done
    fi

    if [ $# -ne 2 ]; then
      helpPage "Unexpected arguments"
    else
      INSTALL_ROOT=`echo $1 | sed -e 's/^\///'`
      USER_NAME=$2

      ARGUMENTS="${ARGUMENTS} ${1} ${2}"
    fi

    if [ ! -r  ${OPTIONS} ]; then
      helpPage "Can not read file '${OPTIONS}'"
    fi
  fi

  # Variables used by child scripts
  export SMALL_DATA
  export USER_NAME
  export INSTALL_ROOT

  set -u

  return ${FUNCTION}
}


# Display a help page and exit the script. If, and only if, an error message is passed then return an error code
# $1: Optional error message
helpPage() {
  local RET

  if [ $# -eq 0 ]; then
    RET=0
  else
    RET=1
    echo $1
    echo ""
  fi

  echo "Usage:"
  echo "  `basename $0` [OPTIONS] <project-root> <user-name>"
  echo "  `basename $0` --clean"
  echo
  echo "Options are:"
  echo "   -h --help             Display this help page and exit."
  echo "   --clean               Remove files from local filesystem and HDFS using details from the last install. Also drops Hive internal tables and views."
  echo "   --options <file>      Provide alternative configuration file. Default is `basename ${OPTIONS}`"
  echo "   --small-data          Install option. Sqoop subset of the data"
  echo
  echo "Used to install GTHR scripts on this machine for a specific user"
  echo
  echo "Return Codes:"
  echo "   0 - Success"
  echo "   1 - Syntax error"
  echo "   2 - Completed with warnings"
  echo "   3 - Configuration error"
  echo "  99 - Internal error"

  exit ${RET}
}


# read a properties file from the local file system
# Warning: It's assumed that the options file is valid (it only contains name/value pairs and comments)
# $1 : The file to read
readPropertiesFile() {
  local FILE_NAME=$1

  if [ ! -r  ${FILE_NAME} ]; then
    helpPage "Can not read '${FILE_NAME}'"
  else
    local PERM=$( stat -c "%a" ${FILE_NAME} )

    chmod +w ${FILE_NAME}
    sed --in-place 's/\r//' ${FILE_NAME}

    chmod ${PERM} ${FILE_NAME}

    . ${FILE_NAME}
  fi
}


# Read all the options
readOptions() {
  # database connection settings - because there is a password we can not default these from the properties file
  readDbOptions WAREHOUSE
  readDbOptions DATAMART
}


# Read the login credentials for a specific database
# $1 : The name of the database
readDbOptions() {
  local DB_USER=${1}_USER
  local DB_PASSWORD=${1}_PASSWORD
  local DB_CONNECTION=${1}_CONNECTION
  local DB=$( echo ${!DB_CONNECTION} | cut -d@ -f2 | cut -d: -f1 )

  read -p "Please enter the $1 (${DB}) username [${USER_NAME}]: " ${DB_USER}

  echo -n "Please enter the $1 (${DB}) password: "
  read -s ${DB_PASSWORD}
  echo "${!DB_PASSWORD}" | sed "s|.|*|g"

  export ${DB_USER}=${!DB_USER:-${USER_NAME}}
  export ${DB_PASSWORD}=${!DB_PASSWORD:-}
}


# Assert that all the expected settings have been set
validateSettings() {
  echo "Validating settings"

  validateSingleSetting WAREHOUSE_CONNECTION
  validateSingleSetting DATAMART_CONNECTION
  validateSingleSetting APPLICATION_OWNER
  validateSingleSetting WAREHOUSE_OWNER
  validateSingleSetting BUCKET_NAME
  validateSingleSetting BUCKET_S3A
  validateSingleSetting ZOOKEEPER_QUORUM
  validateSingleSetting RECIPIENT_LIST
  validateSingleSetting RETRY_MAX_VALUE
  validateSingleSetting RETRY_INTERVAL_VALUE
  validateSingleSetting NAMENODE
  validateSingleSetting SQOOP_OVERLAP_MINUTES
  validateSingleSetting WAREHOUSE_TIMEZONE_OFFSET_MINUTES
  validateSingleSetting SPARK_EXECUTOR_CORES
  validateSingleSetting SPARK_EXECUTOR_MEMORY
  validateSingleSetting SPARK_EXECUTOR_MEMORY_OVERHEAD
  validateSingleSetting SPARK_DRIVER_MEMORY
  validateSingleSetting SPARK_DRIVER_MEMORY_OVERHEAD
  validateSingleSetting SPARK_EXECUTOR_FAILURES
  validateSingleSetting SPARK_YARN_GTHR_QUEUE
  validateSingleSetting SPARK_YARN_GTI_QUEUE
  validateSingleSetting S3_ACCESS_KEY
  validateSingleSetting S3_SECRET_KEY
  validateSingleSetting IMPALA_NAMENODE_HOST
  validateSingleSetting IMPALA_DATANODE_HOST
  validateSingleSetting SSL_ENABLED
  validateSingleSetting HIGH_MEMORY_SPARK_EXECUTOR_CORES
  validateSingleSetting HIGH_MEMORY_SPARK_EXECUTOR_MEMORY
  validateSingleSetting HIGH_MEMORY_SPARK_EXECUTOR_MEMORY_OVERHEAD
  validateSingleSetting HIGH_MEMORY_SPARK_DRIVER_MEMORY
  validateSingleSetting HIGH_MEMORY_SPARK_DRIVER_MEMORY_OVERHEAD
}


# Assert that an install option has been set. These are done in the config file
# $1: Name of variable to validate
validateSingleSetting() {
   if [ -z ${!1+?} ] ; then
      echo "ERROR: Variable '${1}' was not set. Please update ${OPTIONS}"

      exit 3
   else
      echo "  Setting ${1} => ${!1}"
      export $1=${!1}
   fi

   writeInstallOption $1
}


# We are going to dump name value-pairs to a file so we can see how the system was installed.
# Up steam installations can use these files to configure themselves
# $1: name of variable
writeInstallOption() {
  echo "$1=${!1}" >> ${INSTALL_DATA}
}


# Create the properties file used by writeInstallOption
createInstallData() {
  if [ -e  ${INSTALL_DATA} ]; then
    rm -f ${INSTALL_DATA}
  fi

  echo "# GTHR Install started on `hostname` at `date | tr -s " "`" > ${INSTALL_DATA}
  echo "" >>  ${INSTALL_DATA}

  writeInstallOption INSTALL_ROOT
  writeInstallOption USER_NAME
  
  echo "INGEST_VERSION=${APPLICATION_VERSION}" >> ${INSTALL_DATA}
}


# Start installing
install() {
  readPropertiesFile ${OPTIONS}
  createInstallData
  additionalConfiguration
  validateSettings
  readOptions
  cleanHdfs
  unpackInstaller
  setupInstaller
  updateHdfsDirectory
  removeMe
  setupLocal
  setupHdfs
  setDbPermission
  setupDatabases
  postInstallClean

  chmod a-w ${INSTALL_DATA}

  echo "Install completed on `date | tr -s " "`. See ${READ_ME} for further information"
}


unpackInstaller() {
  echo "Unpacking"

  cd ${BASE_DIR}

  if [ ! -f ${BUNDLE} ]; then
    echo "ERROR: Can not find application bundle (${BUNDLE})" 2> /dev/null
    exit 1
  fi

  tar -xf ${BUNDLE}
}


setupInstaller() {
  cd ${EXTRACT_DIR}

  for i in `find . -name "*.sh" -type f` ; do
    sed --in-place 's/\r$//' $i
    chmod +x $i 2>&1 >/dev/null
  done
}


cleanHdfs() {
  if hadoop fs -test -e /${INSTALL_ROOT}/${USER_NAME}/trade ; then
    echo "Removing old files from HDFS"

    set -e
    hdfs dfs -rm -r /${INSTALL_ROOT}/${USER_NAME}/trade &>/dev/null

    set +e
  fi
}


updateHdfsDirectory() {
  cd ${EXTRACT_DIR}

  for i in `ls ${EXTRACT_DIR}/hdfs`; do
    if [ -d ${INSTALL_DIR}/$i ]; then
      if [ -f ${INSTALL_DIR}/$i/setup.sh ]; then
        echo "Updating $i files"
        ${INSTALL_DIR}/$i/setup.sh ${ARGUMENTS}

        local RETURN=$?
        if [ ${RETURN} -ne 0 ]; then
          exit ${RETURN}
        fi
      else
        echo "WARNING: Missing configuration script for '${i}' HDFS files" >&2
      fi
    fi
  done
}

removeMe() {
    find ${EXTRACT_DIR} -type f -name remove.me -delete
}


setupLocal() {
  echo "Creating files on local FS"

  cd ${EXTRACT_DIR}

  set -e

  mv hdfs ${BASE_DIR}
  mv scripts ${BASE_DIR}

  set +e
}

setupHdfs() {
  echo "Copy files to HDFS"

  cd ${BASE_DIR}

  set -e

  hdfs dfs -mkdir -p /${INSTALL_ROOT}/${USER_NAME}/trade
  hdfs dfs -put hdfs/* /${INSTALL_ROOT}/${USER_NAME}/trade

  set +e
  hdfs dfs -mkdir /${INSTALL_ROOT}/${USER_NAME}/trade/conf
  hdfs dfs -put /etc/hive/conf.cloudera.hive/hive-site.xml /${INSTALL_ROOT}/${USER_NAME}/trade/conf
  hdfs dfs -put /etc/hbase/conf.cloudera.hbase/hbase-site.xml /${INSTALL_ROOT}/${USER_NAME}/trade/conf
  find /opt/cloudera/parcels -type f -iname "sqoop-site.xml" -print -quit 2>&1 | grep -v "Permission denied" | xargs -t -I myfile hdfs dfs -put myfile /${INSTALL_ROOT}/${USER_NAME}/trade/conf
  find /opt/cloudera/parcels -type f -iname "oraoop-site-template.xml" -print -quit 2>&1 | grep -v "Permission denied" | xargs -t -I myfile hdfs dfs -put myfile /${INSTALL_ROOT}/${USER_NAME}/trade/conf

  echo "Creating Additional HDFS directories"

  if ! hadoop fs -test -e /${INSTALL_ROOT}/${USER_NAME}/trade/staging ; then
    hdfs dfs -mkdir -p /${INSTALL_ROOT}/${USER_NAME}/trade/staging
  fi

  hdfs dfs -chmod -R a+w /${INSTALL_ROOT}/${USER_NAME}/trade/workflows/seeding 2>/dev/null
  hdfs dfs -chmod -R a+w /${INSTALL_ROOT}/${USER_NAME}/trade/staging 2>/dev/null

}


setDbPermission() {
  hdfs dfs -chmod 400 /${INSTALL_ROOT}/${USER_NAME}/trade/option-files/db-connect-datamart.txt
  hdfs dfs -cp /${INSTALL_ROOT}/${USER_NAME}/trade/option-files/db-connect.txt /${INSTALL_ROOT}/${USER_NAME}/trade/option-files/db-connect-176.txt
  hdfs dfs -cp /${INSTALL_ROOT}/${USER_NAME}/trade/option-files/db-connect.txt /${INSTALL_ROOT}/${USER_NAME}/trade/option-files/db-connect-177.txt
  hdfs dfs -chmod 400 /${INSTALL_ROOT}/${USER_NAME}/trade/option-files/db-connect*.txt
  hdfs dfs -chmod 400 /${INSTALL_ROOT}/${USER_NAME}/trade/option-files/db-connect-warehouse-SqoopExport.txt
  chmod 400 ${BASE_DIR}/hdfs/option-files/db-connect-datamart.txt
  chmod 400 ${BASE_DIR}/hdfs/option-files/db-connect.txt
  chmod 400 ${BASE_DIR}/hdfs/option-files/db-connect-warehouse-SqoopExport.txt
  echo "Permissions set db-connect-datamart.txt, db-connect.txt and db-connect-warehouse-SqoopExport.txt"
}


setupDatabases() {
  cd ${BASE_DIR}/scripts

  echo "Updating HBase Schema"
  ./hbase-schema.sh update ${USER_NAME}

  echo "Creating Hive Maps"
  ./hive-maps.sh create ${USER_NAME} ${INSTALL_ROOT} ${BUCKET_S3A}
}


postInstallClean() {
  echo "Removing Temp Files"

  cd ${BASE_DIR}

  rm -rf ${EXTRACT_DIR}
}


clean() {
  echo "Cleaning"

  readPropertiesFile  ${INSTALL_DATA}

  echo "  project-root => ${INSTALL_ROOT}"
  echo "  user-name => ${USER_NAME}"
  
  cd ${BASE_DIR}/scripts
  ./hive-maps.sh clear ${USER_NAME} ${INSTALL_ROOT} ${BUCKET_S3A}
  cd ${BASE_DIR}
  
  cleanFileSystem
  cleanHdfs

  echo "Cleanup completed on `date | tr -s " "`."
  echo
  echo "Note: No data was removed from HBase"
}


cleanFileSystem() {
  echo "Removing old files from local file system"

  cd ${BASE_DIR}

  local FILE_LIST=$(zcat ${BUNDLE} | tar -tv | tr -s " " | cut -d" " -f6 | cut -d "/" -f 2 | sort | uniq)

  for i in ${FILE_LIST} ; do
    if [ -e $i ]; then
      rm -rf ${BASE_DIR}/$i
    fi
  done

  if [ -e ${EXTRACT_DIR} ]; then
    rm -rf ${EXTRACT_DIR}
  fi
}


execute() {
  init $*
  local FUNCTION=$?

  if [ ${FUNCTION} -eq 0 ]; then
    install
  elif [ ${FUNCTION} -eq 1 ]; then
    clean
  elif [ ${FUNCTION} -eq 2 ]; then
    helpPage
  else
    echo "Internal ERROR: unexpected function code: ${FUNCTION}" 2>/dev/null
    return 99
  fi
}

# Add additional configuration into the system
additionalConfiguration() {
  export APPLICATION_OWNER=$( whoami )
  export APPLICATION_OWNER=${APPLICATION_OWNER,,}
}

#           ### Script Entry Point ##

execute $*
exit $?