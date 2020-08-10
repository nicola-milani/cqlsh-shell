#!/bin/bash
source ./message_library.sh

#list of all tables in destination keyspace
function list_tables(){
    CASSANDRA_HOST="cassandra.eu-west-1.amazonaws.com"
    KEYSPACE="datahub"
    ./mycqlsh-utils.sh --shell $CASSANDRA_HOST --yes --keyspace $KEYSPACE --list-tables > ${CASSANDRA_HOST}_${KEYSPACE}_tables
    if [ $? -eq 0 ]; then
        sleep 2
        cat ${CASSANDRA_HOST}_${KEYSPACE}_tables | tr -s ' ' '\n' | grep "\S" > ${CASSANDRA_HOST}_${KEYSPACE}_list_tables
        rm ${CASSANDRA_HOST}_${KEYSPACE}_tables
    else
        echo "EXECUTION WITH ERROR"
    fi
    
    #list of all tables in source keyspace
    CASSANDRA_HOST="172.19.0.1"
    KEYSPACE="voltron"
    ./mycqlsh-utils.sh --shell $CASSANDRA_HOST --yes --keyspace $KEYSPACE --list-tables > ${CASSANDRA_HOST}_${KEYSPACE}_tables
    if [ $? -eq 0 ]; then
        sleep 2
        cat ${CASSANDRA_HOST}_${KEYSPACE}_tables | tr -s ' ' '\n' | grep "\S" > ${CASSANDRA_HOST}_${KEYSPACE}_list_tables
        rm ${CASSANDRA_HOST}_${KEYSPACE}_tables
    else
        echo "EXECUTION WITH ERROR"
    fi
}

#list_tables

#export table from local database
function export_tables(){
    CASSANDRA_HOST="172.19.0.1"
    KEYSPACE="voltron"
    TABLE_LIST="${CASSANDRA_HOST}_${KEYSPACE}_list_tables"
    do_notify "RUNNING" "START EXPORT ALL TABLES in $TABLE_LIST from ${HOST}:$CASSANDRA_HOST and KEYSPACE:$KEYSPACE"
    for TABLE in $(cat $TABLE_LIST); do
        do_notify "RUNNING" "START EXPORT $TABLE"
        message "START EXPORT $TABLE"
        ./mycqlsh-utils.sh --shell $CASSANDRA_HOST  --yes --keyspace $KEYSPACE --export-tables ${TABLE}
    done
    
    do_notify "RUNNING" "EXPORT TABLES"
    message "FINISH EXPORT TABLES"

}

export_tables
