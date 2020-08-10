#!/bin/bash
source ./message_library.sh
source ./configuration_setup

#list of all tables in destination keyspace
function list_tables(){
    CASSANDRA_HOST=$1
    KEYSPACE=$2
    $(pwd)/mycqlsh-utils.sh --shell $CASSANDRA_HOST --yes --keyspace $KEYSPACE --list-tables > ${CASSANDRA_HOST}_${KEYSPACE}_tables
    if [ $? -eq 0 ]; then
        sleep 2
        cat ${CASSANDRA_HOST}_${KEYSPACE}_tables | tr -s ' ' '\n' | grep "\S" > ${CASSANDRA_HOST}_${KEYSPACE}_list_tables
        rm ${CASSANDRA_HOST}_${KEYSPACE}_tables
    else
        echo "EXECUTION WITH ERROR"
    fi
}


#export table from local database
function export_tables(){
    CASSANDRA_HOST=$1
    KEYSPACE=$2
    TABLE_LIST="${CASSANDRA_HOST}_${KEYSPACE}_list_tables"
    do_notify "RUNNING" "START EXPORT ALL TABLES in $TABLE_LIST from ${HOST}:$CASSANDRA_HOST and KEYSPACE:$KEYSPACE"
    for TABLE in $(cat $TABLE_LIST); do
        do_notify "RUNNING" "START EXPORT $TABLE"
        message "START EXPORT $TABLE"
        $(pwd)/mycqlsh-utils.sh --shell $CASSANDRA_HOST  --yes --keyspace $KEYSPACE --export-tables ${TABLE}
    done
    
    do_notify "RUNNING" "EXPORT TABLES"
    message "FINISH EXPORT TABLES"

}


#TODO
# export header for each table to single file
# remove header from each main table
function export_headers(){
    CASSANDRA_HOST=$1
    KEYSPACE=$2
    TABLE_LIST="${CASSANDRA_HOST}_${KEYSPACE}_list_tables"
    if [ ! -f $TABLE_LIST ]; then
        do_notify "ERROR" "EXPORT LIST NOT EXYST"
        exit 1
    fi
    do_notify "RUNNING" "START EXPORT ALL TABLES HEADERS"
    for TABLE in $(cat $TABLE_LIST); do
        TABLE_FILE="$(pwd)/MCS_FOLDER/raw/${CASSANDRA_HOST}_${KEYSPACE}_${TABLE}.csv"
        cat $TABLE_FILE | head -1 > ${TABLE_FILE}_HEADER_0.csv
        sed -i '1d' $TABLE_FILE
        message "DONE $TABLE_FILE"
    done
}

#export_headers

# shuffle table
function shufle_table(){
    CASSANDRA_HOST=$1
    KEYSPACE=$2
    TABLE_LIST="${CASSANDRA_HOST}_${KEYSPACE}_list_tables"
    if [ ! -f $TABLE_LIST ]; then
        do_notify "ERROR" "EXPORT LIST NOT EXYST"
        exit 1
    fi
    BASE_FOLDER="$(pwd)/MCS_FOLDER"
    SOURCE_FOLDER="$(pwd)/MCS_FOLDER/raw/source"
    SHUF_FOLDER="$(pwd)/MCS_FOLDER/raw/shuf"
    mkdir -p ${SOURCE_FOLDER}
    mkdir -p ${SHUF_FOLDER}
    mv ${BASE_FOLDER}/raw/*.csv* ${SOURCE_FOLDER}/

    for FILE in $(ls ${SOURCE_FOLDER}/* | grep -v HEADER | sort -V ); do
        do_notify "RUNNING" "START SHUFLE ALL TABLES for $FILE"
        shuf -o ${SHUF_FOLDER}/${FILE##*/} < ${FILE}
        rm ${FILE}
    done
}

#shufle_table

# get statistics
function getStatistics(){
    CASSANDRA_HOST=$1
    KEYSPACE=$2
    TABLE_LIST="${CASSANDRA_HOST}_${KEYSPACE}_list_tables"
    STATISTICS_NAME=${CASSANDRA_HOST}_${KEYSPACE}_statistics
    SOURCE="$(pwd)/MCS_FOLDER/raw/shuf"

    echo -e "Table name\tN of row\tNoRow (splitted files)\tAverage Size of row (from 10k lines) in bytes\tMax size of row (from 10k line sample) in bytes\tSize of table (kb)" > $STATISTICS_NAME
    for d in $(cat $TABLE_LIST); do
        MERGED="$SOURCE/${CASSANDRA_HOST}_${KEYSPACE}_${d}_merged.csv"
        for f in $(ls $SOURCE/*${d}* | sort -V); do cat $f; done > ${MERGED}
        AVERAGES=$(awk -F, 'BEGIN {samp=10000;max=-1;}{if(NR>1){len=length($0);t+=len;avg=t/NR;max=(len>max ? len : max)}}NR==samp{exit}END{printf("{lines: %d, average: %d bytes, max: %d bytes}\n",NR,avg,max);}' ${MERGED})
        TOTAL_SIZE=$(cat $MERGED | wc -l)
        AVERAGE_SIZE=$(echo $AVERAGES | cut -d ":" -f3 | cut -d "," -f1)
        MAX_SIZE=$(echo $AVERAGES | cut -d ":" -f4 | cut -d "}" -f1 )
        #echo "Size of table $d: $(wc -l $folder/${d}_merged)" >> statistics
        echo -e "$d\t$(wc -l $MERGED | cut -d " " -f1 )\t${TOTAL_SIZE}\t${AVERAGE_SIZE}\t${MAX_SIZE}\t$(du -k ${SOURCE}/${MERGED} | cut -f1)" >> $STATISTICS_NAME
    done
}

#TODO
#import table

#call function
list_tables $SOURCE_SERVER $SOURCE_KEYSPACE
list_tables $TARGET_SERVER $TARGET_KEYSPACE
export_tables $SOURCE_SERVER $SOURCE_KEYSPACE
export_headers $SOURCE_SERVER $SOURCE_KEYSPACE
shufle_table $SOURCE_SERVER $SOURCE_KEYSPACE
getStatistics $SOURCE_SERVER $SOURCE_KEYSPACE