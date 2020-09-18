#!/bin/bash

fr=$1
to=$2

while [ "$fr" -le "$to" ]
do
	spark-submit extractRawVertices.py --query_date $fr --data_date 20200814 > log_$fr
	hadoop fs -put log_$fr /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/hgy/
	fr=$(date -d "${fr}+1days" +%Y%m%d)
done