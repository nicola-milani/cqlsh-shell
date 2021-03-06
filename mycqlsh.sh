#!/bin/bash
source ./message_library.sh
source ./configuration_setup

mkdir -p ${SOURCE_FOLDER}
mkdir -p ${SHUF_FOLDER}


#list of all tables in destination keyspace
function list_tables(){
    CASSANDRA_HOST=$1
    KEYSPACE=$2
    message "export list of tables in $CASSANDRA_HOST $KEYSPACE"
    
    $(pwd)/mycqlsh-utils.sh --shell $CASSANDRA_HOST --yes --keyspace $KEYSPACE --list-tables > ${CASSANDRA_HOST}_${KEYSPACE}_tables
    if [ $? -eq 0 ]; then
        sleep 2
        cat ${CASSANDRA_HOST}_${KEYSPACE}_tables | tr -s ' ' '\n' | grep "\S" > ${CASSANDRA_HOST}_${KEYSPACE}_list_tables
        rm ${CASSANDRA_HOST}_${KEYSPACE}_tables
    else
        echo "EXECUTION WITH ERROR"
    fi
    
}

#    N=$(getconf _NPROCESSORS_ONLN)
#(
#for thing in a b c d e f g; do
#   ((i=i%N)); ((i++==0)) && wait
#   task "$thing" &
#done
#)


#export table from local database
function export_tables(){
    CASSANDRA_HOST=$1
    KEYSPACE=$2
    RETRY_TABLE_LIST="${CASSANDRA_HOST}_${KEYSPACE}_list_table_retry"
    message "export content of tables in $CASSANDRA_HOST $KEYSPACE"
    
    rm -f ${MCS_FOLDER}/${RETRY_TABLE_LIST}
    rm -f ${MCS_FOLDER}/${RETRY_TABLE_LIST}.loop
    
    TABLE_LIST="${CASSANDRA_HOST}_${KEYSPACE}_list_tables"
    if [ $(wc -l $TABLE_LIST | cut -d " " -f1 ) -lt 1 ]; then
        error_message "Empty file list"
    fi
    do_notify "RUNNING" "START EXPORT ALL TABLES in $TABLE_LIST from HOST:$CASSANDRA_HOST and KEYSPACE:$KEYSPACE"
    for TABLE in $(cat $TABLE_LIST); do
        do_notify "RUNNING" "START EXPORT $TABLE"
        message "START EXPORT $TABLE"
        $(pwd)/mycqlsh-utils.sh --shell $CASSANDRA_HOST  --yes --keyspace $KEYSPACE --export-table ${TABLE}
      #  pids[${i}]=$!
        #sleep 10
    done
    
    for TABLE in $(cat $TABLE_LIST); do
        OUTPUTFILE="${SOURCE_FOLDER}/${CASSANDRA_HOST}_${KEYSPACE}_${TABLE}.csv"
        if [ ! -f ${OUTPUTFILE} ] || [ $(wc -l ${OUTPUTFILE} | cut -d " " -f1 ) -lt 1 ]; then 
            touch ${MCS_FOLDER}/$RETRY_TABLE_LIST 
            echo $TABLE >> ${MCS_FOLDER}/$RETRY_TABLE_LIST 
        fi
    done

    do_notify "RUNNING" "START RETRY EXPORT FOR SOME TABLES in $TABLE_LIST from HOST:$CASSANDRA_HOST and KEYSPACE:$KEYSPACE"
    message "RETRY"
    if [ -f ${MCS_FOLDER}/$RETRY_TABLE_LIST ]; then
        mv ${MCS_FOLDER}/$RETRY_TABLE_LIST ${MCS_FOLDER}/${RETRY_TABLE_LIST}.loop
        for TABLE in $(cat  ${MCS_FOLDER}/${RETRY_TABLE_LIST}.loop); do
            do_notify "RUNNING" "START EXPORT $TABLE"
            message "START EXPORT $TABLE in RETRY mode"
            $(pwd)/mycqlsh-utils.sh --shell $CASSANDRA_HOST  --yes --keyspace $KEYSPACE --export-table ${TABLE}
            OUTPUTFILE="$SOURCE_FOLDER/${CASSANDRA_HOST}_${KEYSPACE}_${TABLE}.csv"
            if [ ! -f ${OUTPUTFILE} ] || [ $(wc -l ${OUTPUTFILE}) -lt 1 ]; then
                touch ${MCS_FOLDER}/$RETRY_TABLE_LIST
                echo $TABLE >> ${MCS_FOLDER}/$RETRY_TABLE_LIST
            fi
        done
    fi
    message "FINISH EXPORT TABLES"
}


#TODO
# export header for each table to single file
# remove header from each main table
#function export_headers(){
#    CASSANDRA_HOST=$1
#    KEYSPACE=$2
#    message "export header of tables in $CASSANDRA_HOST $KEYSPACE and remove header from main table"
#
#    TABLE_LIST="${CASSANDRA_HOST}_${KEYSPACE}_list_tables"
#    if [ ! -f $TABLE_LIST ]; then
#        do_notify "ERROR" "EXPORT LIST NOT EXYST"
#        exit 1
#    fi
#    do_notify "RUNNING" "START EXPORT ALL TABLES HEADERS"
#    for TABLE in $(cat $TABLE_LIST); do
#        TABLE_FILE="$(pwd)/MCS_FOLDER/raw/${CASSANDRA_HOST}_${KEYSPACE}_${TABLE}.csv"
#        cat $TABLE_FILE | head -1 > ${TABLE_FILE}_HEADER_0.csv
#        sed -i '1d' $TABLE_FILE
#        message "DONE $TABLE_FILE"
#    done
#}


# shuffle table
function shufle_table(){
    CASSANDRA_HOST=$1
    KEYSPACE=$2
    TABLE_LIST="${CASSANDRA_HOST}_${KEYSPACE}_list_tables"
    if [ ! -f $TABLE_LIST ]; then
        do_notify "ERROR" "EXPORT LIST NOT EXYST"
        exit 1
    fi
    #mv ${RAW_FOLDER}/*.csv* ${SOURCE_FOLDER}/
    message "shuffle all file exept the first with header"
    for FILE in $(ls ${SOURCE_FOLDER}/* | grep csv. | sort -V ); do
        do_notify "RUNNING" "START SHUFLE ALL TABLES for $FILE"
        (shuf -o ${SHUF_FOLDER}/${FILE##*/} < ${FILE}; rm ${FILE}) &
    done
    
    for FILE in $(ls ${SOURCE_FOLDER}/* | grep .csv | sort -V ); do
        do_notify "RUNNING" "START SHUFLE ALL HEADER for $FILE"
        (head -n +1 ${FILE} > ${SHUF_FOLDER}/${FILE##*/}; sed 1,1d ${FILE} | shuf >> ${SHUF_FOLDER}/${FILE##*/}; rm ${FILE}) &
    done
}


# get statistics
function getStatistics(){
    CASSANDRA_HOST=$1
    KEYSPACE=$2
    TABLE_LIST="${CASSANDRA_HOST}_${KEYSPACE}_list_tables"
    STATISTICS_NAME=${CASSANDRA_HOST}_${KEYSPACE}_statistics
    
    echo -e "Table name\tN_of_row\tNoRow(splittedFiles)\tAvgSizeRow_bytes\tMax_size_row_bytes\tSize_of_Table_kb" > $STATISTICS_NAME
    for d in $(cat $TABLE_LIST); do
        MERGED="$SHUF_FOLDER/${CASSANDRA_HOST}_${KEYSPACE}_${d}_merged.csv"
        #set -x
        for f in $(ls $SHUF_FOLDER/*${d}* | grep -v "merged" | sort -V); do cat $f; done > ${MERGED}
        #set +x
        AVERAGES=$(awk -F, 'BEGIN {samp=10000;max=-1;}{if(NR>1){len=length($0);t+=len;avg=t/NR;max=(len>max ? len : max)}}NR==samp{exit}END{printf("{lines: %d, average: %d bytes, max: %d bytes}\n",NR,avg,max);}' ${MERGED})
        TOTAL_SIZE=$(cat $MERGED | wc -l)
        AVERAGE_SIZE=$(echo $AVERAGES | cut -d ":" -f3 | cut -d "," -f1)
        MAX_SIZE=$(echo $AVERAGES | cut -d ":" -f4 | cut -d "}" -f1 )
        #echo "Size of table $d: $(wc -l $folder/${d}_merged)" >> statistics
        echo -e "$d\t$(wc -l $MERGED | cut -d " " -f1 )\t${TOTAL_SIZE}\t${AVERAGE_SIZE}\t${MAX_SIZE}\t$(du -k ${MERGED} | cut -f1)" >> $STATISTICS_NAME
    done
}

function import_table(){
    CASSANDRA_HOST=$1
    KEYSPACE=$2
    TABLE_LIST="${3}"
    for table in $(cat $TABLE_LIST); do
        message "Provo ad importare la tabella $table"
        if [ $(cat $SHUF_FOLDER/$(ls $SHUF_FOLDER | grep -v merged | grep ${table}.csv | sort -V | head -1) | wc -l ) -gt 0 ]; then
            for f in $(ls $SHUF_FOLDER | grep -v merged | grep ${table}.csv | sort -V ); do
                message "proviene dai file ${f}"
                if [ "${f:(-3)}" = "csv" ]; then
                    message "estraggo intestazione"
                    HEADER=$(cat $SHUF_FOLDER/$f | head -1 | tr -d '\r')
                    do_notify "RUNNGIN" "IMPORT $table to $CASSANDRA_HOST:$KEYSPACE"
                    $(pwd)/mycqlsh-utils.sh --shell $CASSANDRA_HOST  --yes --keyspace $KEYSPACE --import-table ${table} --header auto --from /raw/shuf/$(basename $f)
                else
                    $(pwd)/mycqlsh-utils.sh --shell $CASSANDRA_HOST  --yes --keyspace $KEYSPACE --import-table $table --header $HEADER --from /raw/shuf/$(basename $f)
                fi
            done
        else
            error_message "Non ci sono csv per $table"
        fi
    done
    #copy datahub.palinsesto (rete,orainizioeffettiva,codicecontenitore,codicecontenuto,codicematerialeis,codicepassaggio,codiceprodotto,codicesegmento,codicesupporto,codicetargetprogramma,criptato,durataeffettiva,durataeffettivaalframe,durataeffettivabumper,durataeffettivacontenitore,durataeffettivalorda,durataprevistacontenitore,durataprevistapalinsesto,duratasponsorpromo,edizprod,elemprod,fineparte,flagaffogato,flagqualitaconvenienza,idprod,inizioparte,note,orafineeffettiva,orafineeffettivaalframe,orainizioeffettivaalframe,oraprevistacontenitore,oraprevistapalinsesto,posizionesponsorpromo,presenzatesti,programmabilingue,programmaperminori,progressivoparteprogramma,progressivosupportoprogramma,semaforo,sequenzaspotnastro,sottotitolato,tipoaudio,tipoevento,tipooggetto,titoloassemblaggio,titolocontenitore,versprod) from '/raw/voltron_palinsesto/palinsesto_csv_shuffled/shuffled_palinsesto.csv.2*' with header=false;
    
    #$(pwd)/mycqlsh-utils.sh --shell $CASSANDRA_HOST  --yes --keyspace $KEYSPACE --import-table $TABLE_NAME
    
    
    
}


function count_row(){
    CASSANDRA_HOST=$1
    KEYSPACE=$2
    TABLE_LIST="${3}"
    file_output=$4
    echo "SOURCE_TABLE N_ROWS" > $file_output
    for table in $(cat $TABLE_LIST); do
        message "COUNT $table"
        get_pks $CASSANDRA_HOST $KEYSPACE $TABLE_NAME
        $(pwd)/mycqlsh-utils.sh --shell $CASSANDRA_HOST  --keyspace $KEYSPACE --count $table --with-pk ${PRIMARY_KEYS} > /tmp/nrows_$table
        N_ROW=$(cat /tmp/nrows_$table)
        echo $CASSANDRA_HOST.$KEYSPACE.$table ${N_ROW} >> $file_output
        rm -f /tmp/nrows_$table
    done
}

function get_pks(){
    local CASSANDRA_HOST=$1
    local KEYSPACE=$2
    local TABLE_NAME="${3}"
    $(pwd)/mycqlsh-utils.sh --shell $CASSANDRA_HOST  --keyspace $KEYSPACE --describe-table $table > /tmp/temp_mycqlsh
    cat /tmp/temp_mycqlsh | grep "PRIMARY KEY" > /tmp/pks_00904
    if [ $? -eq 0 ]; then
        rm /tmp/temp_mycqlsh
        cat /tmp/pks_00904 | grep "("
        if [ $? -eq 0 ]; then
            PRIMARY_KEYS=$(cat /tmp/pks_00904 | cut -d "(" -f2 | cut -d ")" -f1)
        else
            PRIMARY_KEYS=$(cat /tmp/pks_00904 | xargs | cut -d " " -f1)
        fi
    else
        error_message "NO PRIMARY KEY FOUND"
    fi
    PRIMARY_KEYS=${PRIMARY_KEYS//[[:blank:]]/}
    rm -f /tmp/pks_00904
}

lst="${SOURCE_SERVER}_${SOURCE_KEYSPACE}_list_tables"
target_lst="${TARGET_SERVER}_${TARGET_KEYSPACE}_list_tables"

#do_notify "RUNNING" "EXTRACT LIST OF TABLES FROM $SOURCE_SERVER:$SOURCE_KEYSPACE"
#list_tables $SOURCE_SERVER $SOURCE_KEYSPACE

#do_notify "RUNNING" "EXTRACT LIST OF TABLES FROM $TARGET_SERVER:$TARGET_KEYSPACE"
#list_tables $TARGET_SERVER $TARGET_KEYSPACE

if [ $(cat $target_lst | wc -l | cut -d " " -f1 ) -eq $(cat $lst | wc -l | cut -d " " -f1 ) ]; then
    
   # do_notify "RUNNING" "EXTRACT CONTENTS OF TABLES FROM $SOURCE_SERVER:$SOURCE_KEYSPACE"
    #export_tables $SOURCE_SERVER $SOURCE_KEYSPACE
    
    do_notify "RUNNING" "SHUFLE CONTENTS OF TABLES TO $SHUF_FOLDER"
    shufle_table $SOURCE_SERVER $SOURCE_KEYSPACE
    do_notify "RUNNING" "GET STATISTICS FROM TABLES IN $SHUF_FOLDER"
    getStatistics $SOURCE_SERVER $SOURCE_KEYSPACE
    ##
    #do_notify "IMPORT ALL TABLES IN $TARGET_SERVER:$TARGET_KEYSPACE"
    #import_table $TARGET_SERVER $TARGET_KEYSPACE ${lst}
    #do_notify "COUNT ALL ROW IN EVERY TABLE IN $TARGET_SERVER:$TARGET_KEYSPACE"
    #count_row $TARGET_SERVER $TARGET_KEYSPACE ${lst} ./${TARGET_SERVER}_${TARGET_KEYSPACE}_conteggio
else
    error_message "TARGET KEYSPACE AND SOURCE KEYSPACE ARE NOT EQUALS"
    exit 1
fi