#/bin/bash

CONFIGURATION_FILE="./configuration_setup"
function get_script_dir()
{
    local DIR=""
    local SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
        DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
        SOURCE="$(readlink "$SOURCE")"
        [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    done
    THIS_SCRIPT_DIR="$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )"
    export THIS_SCRIPT_DIR
}
get_script_dir

if [ -f ${CONFIGURATION_FILE} ]; then
   source ${CONFIGURATION_FILE}
   logger "RUNNING:" "FOUND CONFIGURATION FILE"
else
   logger "ERROR:" "CANNOT FOUND CONFIGURATION FILE"
fi

# DEFINE AND CREATE FOLDER PATH FOR LOG
logPath="${THIS_SCRIPT_DIR}/LOGS/"
mkdir -p $logPath
logFile="${logPath}/history.log"

# CREATE OR ROTATE LOG FILE
if [ ! -f ${logFile} ]; then
    touch ${logFile}
fi
#else
#    if [ `du -b ${logFile} | cut -f1` -gt 5 ]; then
#        mv ${logFile} "${logFile}.1"
#    fi
#fi

#
# FUNCTION TO LOG MESSAGE
#

function do_notify {
    #IF EMPTY, SET SYSLOG AS DEFAULT
    if [ ${#destinationsLogs[@]} -lt 1 ]; then
        destinationsLogs=( 1 )
    fi
    date_time=$(date +%Y-%m-%d"_"%H_%M_%S)
    # ITERATE TYPES OF LOG
    for d in ${destinationsLogs[@]}; do
        case $d in
            1)
                # LOG TO SYSLOG
                logger ${appName}"["$$"]" "-" "${@:2}"
                ;;
            2)
                # LOG TO CUSTOM FILE
                do_log "$$ ${date_time} $@"
                ;;
            3)
                # LOG TO TELEGRAM
                MESSAGE="$$ ${date_time} ${@}"
                #do_post_telegram "${MESSAGE}"
                ;;
            4)
                # LOG TO VERBOSE SCREEN
                do_post "$$ ${date_time} $@"
                ;;
            *)
                #UNMANAGED CASE
                echo "unkown"
                ;;
        esac
    done

}

#
# LOG TO TELEGRAM CHANNEL
#
#function do_post_telegram {
#    ${telegramClient} -c ${telegramClientConfigurationFile} -m "[$HOSTNAME] $@" > /dev/null
#}

#
# LOG TO FILETPATH
#
function do_log {
    echo "$$ $@" >> ${logFile}
}

# LOG TO SCREEN
function do_post {
    echo "$@"
}



function message()
{
    local MSG="$1"
    echo -e "\e[32m$MSG\e[39m"
}

function error_message()
{
    local MSG="$1"
    echo -e "\e[31m$MSG\e[39m"
}

get_script_dir

export get_script_dir
export do_notify
export do_post
export message
export error_message

#do_notify "RUNNING" "Elapsed time for dump db ${project_test}:  $((end_time - start_time)) sec."
#start_time_t=$(date +%s)
#
#do_notify "RUNNING:" "START BACKUP PROCESS"
#
#date_time=$(date +%Y-%m-%d"_"%H_%M_%S)