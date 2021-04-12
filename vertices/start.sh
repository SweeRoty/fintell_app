#!/bin/bash

#read -p "Please enter the start date for this computation (format: YYYYmmdd): " fr
#read -p "Please enter the end date for this computation (format: YYYYmmdd): " to
#read -p "Please enter the threshold when selecting devices (e.g. 1): " thres

fr=$1
to=$2
thres=$3

spark-submit selectDevices.py --fr $fr --to $to --thres $thres > log_device_selection_$fr
hadoop fs -put log_device_selection_$fr /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/hgy/

device_date=$fr
while [ "$fr" -le "$to" ]
do
	spark-submit extractRawVertices.py --query_date $fr --device_date $device_date > log_raw_vertices_extraction_$fr
	hadoop fs -put log_raw_vertices_extraction_$fr /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/hgy/
	fr=$(date -d "${fr}+1days" +%Y%m%d)
done

fr=$device_date
spark-submit prepareVertices.py --fr $fr --to $to > log_vertices_preparation_$fr
hadoop fs -put log_vertices_preparation_$fr /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/hgy/

spark-submit retrieveSamples.py --data_date $fr --thres $thres > log_sample_preparation_$fr
hadoop fs -put log_sample_preparation_$fr /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/hgy/