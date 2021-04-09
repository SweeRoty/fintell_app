#!/bin/bash

#read -p "Please enter the start date for this computation (format: YYYYmmdd): " fr
#read -p "Please enter the end date for this computation (format: YYYYmmdd): " to
#read -p "Please enter the threshold when selecting devices (e.g. 1): " thres

fr=$1
to=$2
thres=$3

spark-submit extractEdges.py --fr $fr --to $to --thres $thres > log_edge_extraction_$fr
hadoop fs -put log_edge_extraction_$fr /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/hgy/