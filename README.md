# mycqlsh

This script support your migration process from local cassandra to Amazon Cassandra Services

## mycqlsh.sh

this script is a "macro template" for mycqlsh-utils.sh. You can customize template for your requirements
- get list of table in a keyspace
- dump table content in a CSV (require list of table)
- import dump to table (require list of table)


## mycqlsh-utils.sh
tool for manage connection to cassandra

## configuration setup
Edit configuration setup for mycqlsh.sh and mycqlsh-utils.sh

## TODO:
- support different schema source,target
- support multithreading
- import and export schema


