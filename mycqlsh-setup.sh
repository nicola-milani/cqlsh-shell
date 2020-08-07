#!/bin/bash
#author: Nicola Milani

if [ -f /etc/mycqlsh.conf ]; then
    source /etc/mycqlsh.conf
elif [ -f ./mycqlsh.conf ]; then
    source ./mycqlsh.conf
else
    echo "configuration file not found"
    exit 1
fi

MCS_FOLDER="${MAIN_CONF}/MCS_FOLDER"

UNSAVE=0

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
    else
        echo "docker is correct installed and running"
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
        echo "Digit q to exit"
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

function setup() {
    local SERVER=$1
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
            echo "credential file was found for $SERVER"
            load_credential ${MCS_FOLDER}/.credentials_$SERVER
        else
            read_credential ${MCS_FOLDER}/.credentials_$SERVER $SERVER
        fi
    else
        if [ "$SERVER" = "CLEAN" ]; then
            read_credential
        else
            if [ -f ${MCS_FOLDER}/.credentials* ]; then
                echo "Credential files was found, load it. If you want to clean your credentials, run $0 with --rmauth option or --clean"
                #progress-bar 5
                load_credentials
            else
                read_credential
            fi
        fi
    fi
}

function running() {
    echo "Creating temporary directory with configuration setup named MCS_FOLDER in your current directory"
    mkdir -p ${MCS_FOLDER}/raw

    if [ ! -f ${MCS_FOLDER}/AmazonRootCA1.pem ]; then
        curl -o ${MCS_FOLDER}/AmazonRootCA1.pem https://www.amazontrust.com/repository/AmazonRootCA1.pem -O
    fi

    if [ ! -f ${MCS_FOLDER}/AmazonRootCA1.pem ]; then
        echo "Unable to download file from Amazon Services, check your internet settings or try to download it from https://www.amazontrust.com/repository/AmazonRootCA1.pem"
        exit 1
    fi

    #create cqlshrc in MCS_FOLDER
    echo "create cqlshrc file in ${MCS_FOLDER}"

    cat <<EOF >${MCS_FOLDER}/cqlshrc
[connection]
port = 9142
factory = cqlshlib.ssl.ssl_transport_factory

[ssl]
validate = true
certfile = /root/.cassandra/AmazonRootCA1.pem

[copy-from]
CHUNKSIZE=30
INGESTRATE=500
MAXINSERTERRORS=-1
MAXPARSEERRORS=-1
MINBATCHSIZE=1
MAXBATCHSIZE=10

[copy]
NUMPROCESSES=16
MAXATTEMPTS=25

[csv]
field_size_limit=999999

EOF

    #"create docker-compose file with selected options"
    echo "create docker-compose file with selected options"
    cat <<EOF >${SERVER}_docker-compose.yaml
version: '3.7'

services:

  cqlsh:
    image: cassandra:3.11
    entrypoint: cqlsh $MCS_HOST $MCS_PORT -u "$MCS_USERNAME" -p "$MCS_PASSWORD" $MCS_SSL --connect-timeout="$MCS_CTIMEOUT" --request-timeout="$MCS_RTIMEOUT"
    volumes:
      - ${MCS_FOLDER}/AmazonRootCA1.pem:/root/.cassandra/AmazonRootCA1.pem
      - ${MCS_FOLDER}/cqlshrc:/root/.cassandra/cqlshrc
      - ${MCS_FOLDER}/raw:/raw
EOF

    #clear
    if [ $SAVE="Y" ]; then
        echo "Now all is ok, your setup files are in $(pwd)/MCS_FOLDER. Your credentials informations are saved in $(pwd)/MCS_FOLDER/.credentials file"
    else
        echo "Now all is ok, your setup files are in $(pwd)/MCS_FOLDER. No credentials informations are saved"
    fi
    echo "After few minutes, your MCS console will running in a secure container."
    echo "You can use \"/raw\" folder inside the container to move file between host and console. The \"/raw\" folder is in $(pwd)/MCS_FOLDER"

    echo "waiting..."
    progress-bar 2

    docker-compose -f ${SERVER}_docker-compose.yaml run --rm cqlsh

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
    Options:
        --shell   connect
    Get info
        --version print current version
        --list-auth print list of credentials by server
        --help    print current message
    Edit
        --rmauth  remove .credentials
        --clean   remove all temporary files
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
      - ./MCS_FOLDER/AmazonRootCA1.pem:/root/.cassandra/AmazonRootCA1.pem
      - ./MCS_FOLDER/cqlshrc:/root/.cassandra/cqlshrc
      - ./MCS_FOLDER/raw:/raw
EOF
    docker-compose run --entrypoint bash --rm cqlsh
}

function main() {
    echo $2 $3 $4 $5
    if [ "$2" = "cmd" ]; then
        echo "Running a cqlsh with bash console..."
        entrypoint_cmd
    else
        #exit 1
        setup $SERVER
        yn="Y"
        while true; do
            echo "Your setup: "
            echo "cqlsh $MCS_HOST $MCS_PORT -u "$MCS_USERNAME" -p "$MCS_PASSWORD" $MCS_SSL --connect-timeout=$MCS_CTIMEOUT --request-timeout=$MCS_RTIMEOUT"
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

for i in "$@"; do
    case $i in
    -h|--help)
        usage
        exit 0
        ;;
    -v|--version)
        echo "Version: $VERSION"
        exit 0
        ;;
    -la|--list-auth)
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
    --shell)
        shift
        SERVER=$1
        main $SERVER $2 $3 $4 $5
        exit 0
        ;;
    *) 
    usage
    break
    esac
done
