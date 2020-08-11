#!/bin/bash
#author: Nicola Milani

if [ -f ./configuration_setup ]; then
    source ./configuration_setup
else
    echo "configuration setup not found"
    exit 1
fi

if [ -f ./message_library.sh ]; then
    source ./message_library.sh
else
    echo "library not found"
    exit 1
fi

VERSION=0.2.1
PROGNAME=${0##*/}
SHORTOPTS="hvyck:"
LONGOPTS="help,version,list-auth,rm-auth,clean,keyspace:,shell:,connect,cmd,list-tables,entrypoint:,export-table:,yes,export-schema,import-schema,import-table:,header:,from:"
UNIQUEOPTS="help,version,list-authm,rm-auth,clean,cmd,list-tables,export-schema,import-schema,export-table,import-table"
SESSION="cqlsh"
ARGS=$(getopt -s bash --options $SHORTOPTS --longoptions $LONGOPTS --name $PROGNAME -- "$@" )
eval set -- "$ARGS"
#execution string for cqlsh, must be empty
EXECUTE=""
#OPTION TO SAVE credentials, 0 for none
UNSAVE=0
#LIST OF TABLES to SAVE
TABLES_LIST=""
#ARGUMENT for action to keyspace
MODE=""

#BUILD STRING EXECUTE
function build_execute(){
    #copy one table from keyspace to local
    case $MODE in
        list-tables)
           # echo "list tables on keyspace"
            local text='--execute "describe tables;"'
            ;;
        export-table)
            #echo "export_table"
            local text="--execute \"COPY ${KEYSPACE}.${TABLE_NAME} TO '/raw/${SERVER}_${KEYSPACE}_${TABLE_NAME}.csv' WITH HEADER=true AND PAGETIMEOUT=40 AND MAXOUTPUTSIZE=100000\""
            ;;
        import-table)
            echo "import_table"
            if [ "$HEADER" = "none" ]; then
                local text="--execute \"consistency local_quorum; COPY ${KEYSPACE}.${TABLE_NAME} from '$TABLE_FROM' WITH HEADER=true;\""
            else
                message "senza intestazione"
                local text="--execute \"consistency local_quorum; COPY ${KEYSPACE}.${TABLE_NAME} ($HEADER) from '$TABLE_FROM' WITH HEADER=true;\""
            fi
            #copy datahub.palinsesto  from '/raw/voltron_palinsesto/palinsesto_csv_shuffled/shuffled_palinsesto.csv.2*' with header=false;
            ;;
        *) 
            ;;
    esac


    EXECUTE=${text}
    #echo $EXECUTE
}

function check_env() {
    docker --version >/dev/null 2>&1
    local DOCKER=$?
    docker-compose --version >/dev/null 2>&1
    local DOCKERCOMPOSE=$?
    systemctl is-active --quiet docker
    local DOCKERSERVICE=$?
    if [ $DOCKER != 0 ] || [ $DOCKERCOMPOSE != 0 ] || [ $DOCKERSERVICE != 0 ]; then
        echo "docker is not installed or need attention"
        exit 1
    fi
}

function progress-bar() {
    local duration=${1}
    already_done() {
        for ((done = 0; done < $elapsed; done++)); do
            printf ""
        done
    }
    remaining() { for ((remain = $elapsed; remain < $duration; remain++)); do printf " "; done; }
    percentage() { printf "| %s%%" $(((($elapsed) * 100) / ($duration) * 100 / 100)); }
    clean_line() { printf "\r"; }

    for ((elapsed = 1; elapsed <= $duration; elapsed++)); do
        already_done
        remaining
        percentage
        sleep 1
        clean_line
    done
    #clean_line
}

function export_credentials() {
    FILENAME=$1
    #progress-bar 2
    mkdir -p ${MCS_FOLDER}/raw
    touch ${MCS_FOLDER}/.credentials_${FILENAME}
    cat <<EOF >${MCS_FOLDER}/.credentials_${FILENAME}
MCS_SSL="$MCS_SSL"
MCS_PORT="$MCS_PORT"
MCS_USERNAME="$MCS_USERNAME"
MCS_PASSWORD="$MCS_PASSWORD"
MCS_HOST="$MCS_HOST"
MCS_CTIMEOUT="$MCS_CTIMEOUT"
MCS_RTIMEOUT="$MCS_RTIMEOUT"
SAVE="$SAVE"

EOF

}

function load_credentials() {
     CREDENTIAL_FILE=""
     while true; do
        echo "Digit q to exit, or c to force custom"
        select ITEM in $(ls $MCS_FOLDER/.credentials*); do
            case $ITEM in
            *)
                echo "Select credential to use: $ITEM"
                break
                ;;

            esac
        done
        if [ "$REPLY" = "q" ]; then
            echo "bye bye"
            exit 0
        fi
        if [ "$REPLY" = "c" ]; then
            read_credential
            break
        fi
        if [ ! -z $ITEM ]; then
            CREDENTIAL_FILE=${ITEM}
            break;
        else
            echo "Selection not valid"
            continue
        fi
    done
    source ${CREDENTIAL_FILE}
}

function load_credential(){
    source $1
}

function read_credential(){
    SERVER=$2
    if [ ! -z $SERVER ] && [ ! "$SERVER" = "CLEAN" ]; then
        MCS_HOST=$SERVER
    else
        MCS_HOST=""
    fi
    read -e -i "$MCS_HOST" -p "Connection host: " MCS_HOST
    MCS_PORT="9142"
    read -e -i "$MCS_PORT" -p "Host port <default 9142>: " input
    MCS_PORT="${input:-$MCS_PORT}"
    read -p "Username: " -a MCS_USERNAME
    read -p "Password: " -a MCS_PASSWORD
    MCS_CTIMEOUT="6000"
    read -e -i "$MCS_CTIMEOUT" -p "Connect timeout: " MCS_CTIMEOUT
    MCS_RTIMEOUT="6000"
    read -e -i "$MCS_RTIMEOUT" -p "Request timeout: " MCS_RTIMEOUT
    yn="Y"
    while true; do
        read -e -i "$yn" -p "Do you need ssl? <default Y> " yn
        case $yn in
        [Yy]*)
            MCS_SSL="--ssl"
            break
            ;;
        [Nn]*)
            MCS_SSL=" "
            break
            ;;
        *) echo "Please answer yes (Y or y) or no (N or n)." ;;
        esac
    done
    SAVE="N"
    yn="N"
    while true; do
        read -e -i "$yn" -p "Save credential for future:? <default N> " yn
        case $yn in
        [Yy]*)
            SAVE="Y"
            export_credentials ${MCS_HOST}
            break
            ;;
        [Nn]*)
            SAVE="N"
            break
            ;;
        *) echo "Please answer yes (Y or y) or no (N or n)." ;;
        esac
    done
}

#
# setup configuration file for running connection
#
function setup() {
    check_env
    if [[ "$UNSAVE" -eq 1 ]]; then
        if [ -f ${MCS_FOLDER}/.credentials_$SERVER ]; then
            echo "Remove old credentials file ${MCS_FOLDER}/.credentials_$SERVER"
            rm -f ${MCS_FOLDER}/.credentials_$SERVER
            #clean
        fi
    fi
    if [ ! -z $SERVER ]; then 
        if [ -f ${MCS_FOLDER}/.credentials_$SERVER ]; then
            #echo "credential file was found for $SERVER"
            #echo "loading ${MCS_FOLDER}/.credentials_$SERVER"
            load_credential ${MCS_FOLDER}/.credentials_$SERVER
        else
            read_credential ${MCS_FOLDER}/.credentials_$SERVER $SERVER
        fi
    else
        if [ "$SERVER" = "CLEAN" ]; then
            read_credential
        else
            ls -al ${MCS_FOLDER}/.credentials* > /dev/null 2>&1
            if [ $? -eq 0 ]; then
                echo "Credential files was found, load it. If you want to clean your credentials, run $0 with --rmauth option or --clean"
                #progress-bar 5
                load_credentials
            else
                read_credential
            fi
        fi
    fi

   # echo "Creating temporary directory with configuration setup named MCS_FOLDER in your current directory"
    mkdir -p ${MCS_FOLDER}/raw

    if [ ! -f ${MCS_FOLDER}/AmazonRootCA1.pem ]; then
        curl -o ${MCS_FOLDER}/AmazonRootCA1.pem https://www.amazontrust.com/repository/AmazonRootCA1.pem -O
    fi

    if [ ! -f ${MCS_FOLDER}/AmazonRootCA1.pem ]; then
        echo "Unable to download file from Amazon Services, check your internet settings or try to download it from https://www.amazontrust.com/repository/AmazonRootCA1.pem"
        exit 1
    fi
    #create cqlshrc in MCS_FOLDER
    #echo "create cqlshrc file in ${MCS_FOLDER}"

    cat <<EOF >${MCS_FOLDER}/cqlshrc
[connection]
port = 9142
factory = cqlshlib.ssl.ssl_transport_factory

[ssl]
validate = true
certfile = /root/.cassandra/AmazonRootCA1.pem

[copy-from]
CHUNKSIZE=30
INGESTRATE=1500
MAXINSERTERRORS=-1
MAXPARSEERRORS=-1
MINBATCHSIZE=1
MAXBATCHSIZE=25

[copy]
NUMPROCESSES=4
MAXATTEMPTS=25

[csv]
field_size_limit=999999

EOF

   # echo $MCS_HOST
    if [ ! -z $MCS_HOST ]; then
        CUSTOM=$MCS_HOST
    else
        CUSTOM=$SERVER
    fi

   # echo $MCS_KEYSPACE
    if [ ! -z $KEYSPACE ]; then
        MCS_KEYSPACE="--keyspace $KEYSPACE"
    else
        MCS_KEYSPACE=""
    fi

    build_execute $MODE

   #exit 1
    #"create docker-compose file with selected options"
    #echo "create docker-compose file with selected options"
    cat <<EOF >${CUSTOM}_docker-compose.yaml
version: '3.7'

services:

  cqlsh_${MCS_HOST}:
    image: cassandra:3.11
    entrypoint: cqlsh $MCS_HOST $MCS_PORT -u "$MCS_USERNAME" -p "$MCS_PASSWORD" $MCS_SSL --connect-timeout="$MCS_CTIMEOUT" $MCS_KEYSPACE --request-timeout="$MCS_RTIMEOUT" $EXECUTE
    volumes:
      - ${MCS_FOLDER}/AmazonRootCA1.pem:/root/.cassandra/AmazonRootCA1.pem
      - ${MCS_FOLDER}/cqlshrc:/root/.cassandra/cqlshrc
      - ${MCS_FOLDER}/raw:/raw

EOF


    #clear
    #if [ $SAVE="Y" ]; then
    #    echo "Now all is ok, your setup files are in $(pwd)/MCS_FOLDER. Your credentials informations are saved in $(pwd)/MCS_FOLDER/.credentials file"
    #else
    #    echo "Now all is ok, your setup files are in $(pwd)/MCS_FOLDER. No credentials informations are saved"
    #fi
   # echo "After few minutes, your MCS console will running in a secure container."
   # echo "You can use \"/raw\" folder inside the container to move file between host and console. The \"/raw\" folder is in $(pwd)/MCS_FOLDER"

   # echo "waiting..."
    #progress-bar 2
}

function running() {
    if [ "$MODE" = "list-tables" ] || [ "$MODE" = "shell" ]; then
        docker-compose -f ${CUSTOM}_docker-compose.yaml run --rm cqlsh_${CUSTOM} 
    else
        #docker-compose -f ${CUSTOM}_docker-compose.yaml run --rm cqlsh_${CUSTOM} 2>&1 > /dev/null
        docker-compose -f ${CUSTOM}_docker-compose.yaml run --rm cqlsh_${CUSTOM} 
    fi
}

function logo() {

    echo " _____ _____ __    _____ _____        _           _     "
    echo "|     |     |  |  |   __|  |  |   ___|_|_____ ___| |___ "
    echo "|   --|  |  |  |__|__   |     |  |_ -| |     | . | | -_|"
    echo "|_____|__  _|_____|_____|__|__|  |___|_|_|_|_|  _|_|___|"
    echo "         |__|                                |_|        "
    echo ""
}

function usage() {
    logo
    cat <<EOF
Usage: $PROGNAME <options>
    --cmd
    --shell <hostname or ip> [-y|--yes] -c|--connect connect to cassandra server
    --shell <hostname or ip> [--yes] [--keyspace: <keyspace_name>] -c|--connect
    --shell <hostname or ip> [--yes] --keyspace: <keyspace_name> --list-tables
    --shell <hostname or ip> [--yes] --keyspace: <keyspace_name> --export-schema
    --shell <hostname or ip> [--yes] --keyspace: <keyspace_name> --import-schema
    --shell <hostname or ip> [--yes] --keyspace: <keyspace_name> --export-table: <table name>
    --shell <hostname or ip> [--yes] --keyspace: <keyspace_name> --import-table: <table file>
    --list-auth print list of credentials by server
    --rmauth  remove .credentials
    --clean   remove all temporary files
    -v|--version print current version
    -h|--help    print current message

EOF

}

function entrypoint_cmd(){
    check_env
    cat <<EOF >docker-compose.yaml
version: '3.7'

services:

  cqlsh:
    image: cassandra:3.11
    entrypoint: cqlsh
    volumes:
      - ${MCS_FOLDER}/AmazonRootCA1.pem:/root/.cassandra/AmazonRootCA1.pem
      - ${MCS_FOLDER}/cqlshrc:/root/.cassandra/cqlshrc
      - ${MCS_FOLDER}/raw:/raw
EOF
    docker-compose run --entrypoint bash --rm cqlsh
}


#
# function to review setup configuration if all is ok, launch running
#
function review() {

    if [ "$yn" = "y" ]; then
        #echo "Execution auto"
        setup
        running
    else
        setup $SERVER
        yn="Y"
        while true; do
            echo "Your setup: "
            if [ ! -z $KEYSPACE ]; then
                echo "cqlsh $MCS_HOST $MCS_PORT -u "$MCS_USERNAME" -p "$MCS_PASSWORD" $MCS_SSL --connect-timeout=$MCS_CTIMEOUT --request-timeout=$MCS_RTIMEOUT --keyspace=$KEYSPACE"
            else
                echo "cqlsh $MCS_HOST $MCS_PORT -u "$MCS_USERNAME" -p "$MCS_PASSWORD" $MCS_SSL --connect-timeout=$MCS_CTIMEOUT --request-timeout=$MCS_RTIMEOUT"
            fi
            read -e -i "$yn" -p "Is it correct? <default Y> " yn
            case $yn in
            [Yy]*)
                running
                break
                ;;
            [Nn]*)
                UNSAVE=1
                setup "CLEAN"
                ;;
            *) echo "Please answer yes (Y or y) or no (N or n)." ;;
            esac
            echo ""
        done
    fi
}

saved_credentials(){
    i=0
    ls -a $MCS_FOLDER/.credentials* > /dev/null 2>&1
    if [ $? -eq 0 ]; then 
        for c in $(ls -a $MCS_FOLDER/.credentials* | sort -V); do
            i=$((i+1))
            echo $i ")" $c
        done
    else
        echo "No credential saved"
    fi
}



#STARTED POINT

SECURE=0
ERRORS=()
IFS=', ' read -r -a ARR_UNIQUEOPTS <<< "$UNIQUEOPTS"
for a in $@
do
    for element in "${ARR_UNIQUEOPTS[@]}"
    do
        if [ $a == "--" ]; then
            continue
        elif [[ $a == --* ]]; then
            if [ ${a#??} == "$element" ]; then
                ERRORS+=($a)
                SECURE=$((SECURE+1))
            fi
        fi
    done
done

if [ $SECURE -gt 1 ]; then
    echo "ERROR: Argument ${ERRORS[0]} is incompatible with ${ERRORS[@]:1} options"
    usage
    exit 1
fi


while true; do
    case $1 in
    -h|--help)
        usage
        exit 0
        ;;
    -v|--version)
        echo "Version: $VERSION"
        exit 0
        ;;
    --list-auth)
        echo "SAVED CREDENTIALS BY SERVER"
        saved_credentials
        exit 0
        ;;
    --rmauth)
        if [ -f $MCS_FOLDER/.credentials ]; then
            rm -rf $MCS_FOLDER/.credentials*
        fi
        exit 0
        ;;
    --clean)
        if [ -d $MCS_FOLDER ]; then
            rm -rf $MCS_FOLDER
        fi
        exit 0
        ;;
    --cmd)
        entrypoint_cmd
        exit 0
        ;;
    --shell)
        MODE=${1#??}
        shift
        SERVER=$1
        shift
        ;;
    -y|--yes)
        yn=y
        shift
        ;;
    -k|--keyspace)
        shift
        KEYSPACE=$1
        #echo $KEYSPACE
        shift
        ;;
    --list-tables)
        MODE=${1#??}
        review $SERVER
        break
        ;;
    --export-schema)
        MODE=${1#??}
        review $SERVER
        ;;
    --import-schema)
        MODE=${1#??}
        review $SERVER
        break;
        ;;
    --export-table)
        MODE=${1#??}
        shift;
        TABLE_NAME=$1
        shift
        review $SERVER
        exit 0
        ;;
    --import-table)
        MODE=${1#??}
        shift;
        TABLE_NAME=$1
        shift
        if [ "$1" = "--header" ]; then
            shift
            HEADER=$1
            echo $HEADER
            shift
            if [ "$1" = "--from" ]; then
                shift
                TABLE_FROM=$1
            else
                error_message "import csv no specificate"
            fi
        fi
        review $SERVER
        exit 0
        ;;
    -c|--connect)
        review $SERVER
        break
        exit 0
        ;;
    *) 
        usage
        break
        ;;
    esac
done
