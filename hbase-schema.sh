#!/usr/bin/env bash

# Parse command line and set up environment variables
# Returns action code:
#   0: Create a new namespace
#   1: Clean existing namespace
#   2: Add missing tables from namespace
#   3: purge data
#   4: status
function init() {
  local ACTION=0
  TABLE_FILTER=
  COMMAND=
  OUTPUT="/dev/null"

  local INSTALL_SCRIPT=$( readlink -m $( type -p $0 ))      # Full path to this script
  SCRIPT_DIR=`dirname ${INSTALL_SCRIPT}`                    # Directory script is run in
  SCHEMA_DIR=${SCRIPT_DIR}/hbase-schema                     # Directory schemas are defined in

  if [ $# -eq 0 ]; then
    helpPage "ERROR: No command specified" >&2
  elif [ "${1^^}" = "CREATE" ]; then
    ACTION=0
  elif [ "${1^^}" = "REMOVE" ]; then
    ACTION=1
  elif [ "${1^^}" = "UPDATE" ]; then
    ACTION=2
  elif [ "${1^^}" = "PURGE" ]; then
    ACTION=3
  elif [ "${1^^}" = "STATUS" ]; then
    ACTION=4
  else
    helpPage "ERROR: Unexpected command ${1}" >&2
  fi

  shift

  # Parse option(s)
  if [ $# -ne 0 ]; then
    while [ "${1:0:1}" = "-" ] ; do
      local OPTION=${1^^}

      if [ "${OPTION}" = "-H" -o "${OPTION}" = "--HELP" -o "${OPTION}" = "-?" ]; then
        helpPage
      elif [ "${OPTION}" = "-V" -o "${OPTION}" = "--VERBOSE" ]; then
        OUTPUT="/dev/tty"
      else
        helpPage "Unexpected option ${OPTION}"
      fi
      shift
    done
  fi

  # final argument must be name space
  if [ $# -eq 0 ] ; then
    helpPage "ERROR: No namespace specified" >&2
  elif [ $# -eq 1 ] ; then
    NAMESPACE=$1
  else
    helpPage "ERROR: Unexpected options: $@" >&2
  fi

  return ${ACTION}
}


# Display a help page and exit the script. If, and only if, an error message is passed then return an error code
# $1: Optional error message
helpPage() {
  local RET

  if [ $# -eq 0 ]; then
    RET=0
  else
    RET=1
    while [ $# -ne 0 ]; do
      echo $1
      shift
    done
    echo ""
  fi

  echo "Usage:"
  echo "  `basename $0` <command> <options> <user-name>"
  echo
  echo "Command are:"
  echo "   create          Create namespace"
  echo "   remove          Remove namespace"
  echo "   update          Update namespace to include any additional tables"
  echo "   purge           remove data from all tables"
  echo "   status          describe the name space"
  echo
  echo "Options are:"
  echo "   -h --help       Display this help page and exit."
  echo "   -v --verbose    Verbose output"
  echo
  echo "Used to manage HBase table schemas"
  echo
  echo "Return Codes:"
  echo "   0 - Success"
  echo "   1 - Syntax error"
  echo "   2 - HBase error"
  echo "  99 - Internal error"

  exit ${RET}
}


# Add a command to ${COMMAND} prior to sending to hbase
# $1: Command to send to hbase
run() {
  if [ ${#COMMAND} -eq 0 ] ; then
    COMMAND="$*"
  else
    COMMAND="${COMMAND}\n$*"
  fi
}


# Launch hbase with the command(s) in ${COMMAND}. Any output from hbase will be sent to standard-out
# ${COMMAND} will be reset
# Return 0 if the command succeeded, or 2 if it failed.
function launch() {
  if [ ${#COMMAND} -eq 0 ]; then
    return 0
  else
    echo -e "${COMMAND}" | hbase shell -n 2>/dev/null

    local RET=$?
    COMMAND=

    if [ ${RET} -eq 0 ]; then
      return 0
    else
      return 2
    fi
  fi
}


# Read tables in the name space. The list of tables is sent to standard out
# Returns: 0 The all tables were listed
#          1 the namespace does not exist
function readTables() {

  run describe_namespace \'${NAMESPACE}\'
  run list \'${NAMESPACE}:.*\'

  local RESULT=`launch`
  local RET=

  echo ${RESULT} | head -n1 | grep -q "^ERROR "
  if [ $? -eq 0 ] ; then                                # Name space not found
    RET=1
  else
    RET=0
    echo -e "${RESULT}" | sed '1,/^$/d' | grep "^${NAMESPACE}:" | sort | uniq
    RET=${PIPESTATUS[0]}
  fi

  return ${RET}
}


# Commands to remove a namespace (and all of the tables it contains) from HBase
function removeSchema() {
  local RET=
  local TABLES=

  TABLES=$(readTables)
  RET=$?

  if [ ${RET} -eq 0 ]; then
    # Disable and remove each table

    for TAB in ${TABLES}; do
      echo "  Removing table ${TAB}"

      run disable \'${TAB}\'
      run drop \'${TAB}\'
    done

    # Remove the namespace
    run drop_namespace \'${NAMESPACE}\'

    launch > ${OUTPUT}
    RET=$?
  fi

  return ${RET}
}


# Add missing tables to namespace
updateSchema() {
  local RET=
  TABLE_FILTER=$(readTables)
  RET=$?

  if [ ${RET} -eq 0 ]; then
    TABLE_FILTER=`echo "${TABLE_FILTER} " | tr "\n" " "`
  elif [ ${RET} -eq 1 ]; then
    createNamespace
  else
    return 2
  fi

  createAllTables

  launch > ${OUTPUT}
  RET=$?

  return ${RET}
}


# Execute the commands to add a new namespace and all of the tables we require
function createSchema() {
  createNamespace
  createAllTables

  launch > ${OUTPUT}
  local RET=$?

  return ${RET}
}


# Create the commands to create a new name space
function createNamespace {
  echo "Creating new name space ${NAMESPACE}"

  run create_namespace \'${NAMESPACE}\'
}


# Create the commands to create the tables
function createAllTables() {
  for FILE in $( ls ${SCHEMA_DIR}/*.txt ); do
     local NUMBER=0

    while read -r LINE || [[ -n "${LINE}" ]]; do
      LINE=$( echo -e ${LINE} | expand | tr -s " " | sed 's/\r//' )
      NUMBER=$(( ${NUMBER} + 1))

      if [ ${#LINE} -le 1 ] || [ "${LINE:0:1}" = "#" ] ; then
         : # skip blank lines and comments
      else
        local TYPE=$( echo ${LINE} | cut -d" " -f1 )
        local NAME=$( echo ${LINE} | cut -d" " -f2 )
        local FAMILIES=$( echo ${LINE} | cut -d" " -f3- )

        if [ "${TYPE^^}" = "TABLE" ]; then
          createTable ${NAME} ${FAMILIES}
        elif [ "${TYPE^^}" = "DELTA" ]; then
          createMirrorAndDeltaTables ${NAME} ${FAMILIES}
        else
          echo "ERROR: ${FILE}:${NUMBER}: Has invalid type ${TYPE}"
          exit 99
        fi
      fi
    done < "${FILE}"
  done
}



# Create the commands to create the tables
# $1: the name of the MIRROR table
# $2: column family name
# $3...$n : optional additional column families
function createMirrorAndDeltaTables() {
  local TABLE=$1

  shift

  createTable ${TABLE} $*
  createTable ${TABLE}_DELTA $*
}


# Create a command to create single table if it's not already in the ${TABLE_FILTER}
# $1: the name of the table
# $2: column family name
# $3...$n : optional additional column families
createTable() {
  echo "${TABLE_FILTER}" | grep -s ":${1} " >/dev/null
  local FOUND=${PIPESTATUS[1]}

  if [ ${FOUND} -ne 0 ] ; then
    local TABLE=$1
    local FAMILIES=
    local NAME=", {NAME=>"
    local COMPRESSION=",COMPRESSION=>'snappy'}"

    echo "  Creating table ${TABLE}"

    shift
    while [ $# -ne 0 ]; do
      FAMILIES+="${NAME}'${1}'${COMPRESSION}"
      shift
    done

    run create \'${NAMESPACE}:${TABLE}\'${FAMILIES}

    TABLE_FILTER="${TABLE_FILTER} ${NAMESPACE}:${TABLE} "
   fi
}


# Remove all data in all tables, but not the tables themselves
purgeTables() {
  local RET=
  local TABLES=

  TABLES=$(readTables)
  RET=$?

  if [ ${RET} -eq 0 ]; then
    for TAB in ${TABLES}; do
      echo "  Purging from table ${TAB}"

      run truncate \'${TAB}\'
    done

    launch > ${OUTPUT}
    RET=$?
  fi

  return ${RET}
}




# Status report
status() {
  local RET=
  local TABLES=

  TABLES=$(readTables)
  RET=$?

  if [ ${RET} -eq 0 ]; then
    local COUNT=$( echo " ${TABLES}" | grep -o "${NAMESPACE}:" | wc -l )

    echo "Namespace ${NAMESPACE} exists with ${COUNT} table(s)"
    echo ""

    for TAB in ${TABLES} ; do
       echo "  ${TAB}"
    done

    echo ""
  elif [ ${RET} -eq 1 ]; then
    echo "Namespace ${NAMESPACE} does not exist"
    RET=0
  else
    echo "ERROR: failed to read HBase" >&2
  fi

  return ${RET}
}



#           ### Script Entry Point ##

init $*
ACTION=$?

if [ ${ACTION} -eq 0 ]; then
  createSchema
  RET=0
elif [ ${ACTION} -eq 1 ]; then
  removeSchema
  RET=$?
elif [ ${ACTION} -eq 2 ]; then
  updateSchema
  RET=$?
elif [ ${ACTION} -eq 3 ]; then
  purgeTables
  RET=$?
elif [ ${ACTION} -eq 4 ]; then
  status
  RET=$?
else
  echo "INTERNAL ERROR. init() = ${ACTION}" >&2
  RET=99
fi

if [ ${RET} -ne 0 ]; then
  echo "Failed with error ${RET}"
fi

exit ${RET}