#!/bin/bash
#author: Nicola Milani

MCS_FOLDER="./MCS_FOLDER"
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
    clean_line
}

function export_credentials() {
    progress-bar 2

    cat <<EOF >${MCS_FOLDER}/.credentials
MCS_SSL="$MCS_SSL"
MCS_PORT="$MCS_PORT"
MCS_USERNAME="$MCS_USERNAME"
MCS_PASSWORD="$MCS_PASSWORD"
MCS_HOST="$MCS_HOST"
SAVE="$SAVE"

EOF

}
function load_credentials() {
    source ${MCS_FOLDER}/.credentials
}

function setup() {
    check_env
    if [[ "$UNSAVE" -eq 1 ]]; then
        if [ -f ${MCS_FOLDER}/.credentials ]; then
            echo "Remove old credentials file ${MCS_FOLDER}/.credentials"
            rm -f ${MCS_FOLDER}/.credentials
            clean
        fi
    fi
    if [ -f ${MCS_FOLDER}/.credentials ]; then
        echo "Credential files was found, load it. If you want to clean your credentials, run $0 with --rmauth option or --clean"
        progress-bar 5
        load_credentials
    else
        read -p "Connection host: " -a MCS_HOST
        MCS_PORT="9142"
        read -e -i "$MCS_PORT" -p "Host port <default 9142>: " input
        MCS_PORT="${input:-$MCS_PORT}"
        read -p "Username: " -a MCS_USERNAME
        read -p "Password: " -a MCS_PASSWORD
        yn="Y"
        while true; do
            read -e -i "$yn" -p "Do you need ssl? <default Y> " yn
            case $yn in
            [Yy]*)
                MCS_SSL="--ssl"
                break
                ;;
            [Nn]*)
                MCS_SSL=""
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
                export_credentials
                break
                ;;
            [Nn]*)
                SAVE="N"
                break
                ;;
            *) echo "Please answer yes (Y or y) or no (N or n)." ;;
            esac
        done
    fi
}

function running() {
    echo "Creating temporary directory with configuration setup named MCS_FOLDER in your current directory"
    mkdir -p ${MCS_FOLDER}/raw
    curl -o ${MCS_FOLDER}/AmazonRootCA1.pem https://www.amazontrust.com/repository/AmazonRootCA1.pem -O

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

EOF

    #"create docker-compose file with selected options"
    echo "create docker-compose file with selected options"
    cat <<EOF >docker-compose.yaml
version: '3.7'

services:

  cqlsh:
    image: cassandra:3.11
    entrypoint: cqlsh $MCS_HOST $MCS_PORT -u "$MCS_USERNAME" -p "$MCS_PASSWORD" $MCS_SSL
    volumes:
      - ${MCS_FOLDER}/AmazonRootCA1.pem:/root/.cassandra/AmazonRootCA1.pem
      - ${MCS_FOLDER}/cqlshrc:/root/.cassandra/cqlshrc
      - ${MCS_FOLDER}/raw:/raw
EOF

    clear
    if [ $SAVE="Y" ]; then
        echo "Now all is ok, your setup files are in $(pwd)/MCS_FOLDER. Your credentials informations are saved in $(pwd)/MCS_FOLDER/.credentials file"
    else
        echo "Now all is ok, your setup files are in $(pwd)/MCS_FOLDER. No credentials informations are saved"
    fi
    echo "After few minutes, your MCS console will running in a secure container."
    echo "You can use \"/raw\" folder inside the container to move file between host and console. The \"/raw\" folder is in $(pwd)/MCS_FOLDER"

    echo "waiting..."
    progress-bar 5

    docker-compose run --rm cqlsh

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
        --help    print current message
    Edit
        --rmauth  remove .credentials
        --clean   remove all temporary files
EOF

}

function main() {
    setup
    yn="Y"
    while true; do
        echo "Your setup: "
        echo "cqlsh $MCS_HOST $MCS_PORT -u "$MCS_USERNAME" -p "$MCS_PASSWORD" $MCS_SSL"
        read -e -i "$yn" -p "Is it correct? <default Y> " yn
        case $yn in
        [Yy]*)
            running
            break
            ;;
        [Nn]*)
            UNSAVE=1
            setup
            ;;
        *) echo "Please answer yes (Y or y) or no (N or n)." ;;
        esac
        echo ""
    done
}

while true; do
    case $1 in
    --help)
        usage
        exit 0
        ;;
    --version)
        echo "Version: $VERSION"
        exit 0
        ;;
    --rmauth)
        if [ -f $MCS_FOLDER/.credentials ]; then
            rm -rf $MCS_FOLDER/.credentials
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
        main
        break
        ;;
    *) 
    usage
    break
    esac
done
