#!/bin/bash

output=$1
input=$2
to_epsg_code=$3

if [ $# -ne 3 ]; then
    echo ERROR: Expected 3 arguements, $# were given
    exit 1
fi

ogr2ogr \
    -f Parquet \
    -t_srs "EPSG:$to_srs" \
    -sql "SELECT * FROM WESM WHERE workunit LIKE 'MN%'" \
    $output $input 