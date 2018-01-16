#!/bin/bash
# USAGE ./export.sh PROJECT_NAME [USER:MBL]
P=$1

if [ "$2" = "USER" ]
then
    SOURCE="_pipe"
    echo "Source USER"
else
    SOURCE=""
    echo "Source MBL"
fi
mysql -B -h vampsdb vamps -e "SELECT * FROM vamps_metadata where project = '$P';" | sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" > metadata_$P.csv

mysql -B -h vampsdb vamps -e "SELECT * FROM vamps_sequences$SOURCE where project = '$P';" |sed "s/'/\'/;s/\t/\",\"/g;s/^/\"/;s/$/\"/;s/\n//g" > sequences_$P.csv
    
    