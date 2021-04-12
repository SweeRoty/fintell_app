#!/bin/bash

fr=$1 #"20200901"
to=$2 #"20200914"
thres=$3 #2799

spark-submit extract_ins_dev_features.py --thres $thres --fr $fr --to $to --is_ins > log_ins_$fr_$to
hadoop fs -put log_ins_$fr_$to /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/hgy/

spark-submit extract_ins_dev_features.py --thres $thres --fr $fr --to $to --is_dev > log_dev_$fr_$to
hadoop fs -put log_dev_$fr_$to /user/hive/warehouse/ronghui_bj.db/etl_v1_2/log/hgy/