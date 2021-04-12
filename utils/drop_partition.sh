#!/bin/bash

tbl=$1
data_date=$2

job="
use ronghui;
set mapreduce.job.queuename=root.ronghui.partner.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

alter table ronghui.hgy_$tbl drop IF EXISTS PARTITION(data_date=$data_date);
"
nohup beeline -e "$job" > log_drop_partition_$tbl &