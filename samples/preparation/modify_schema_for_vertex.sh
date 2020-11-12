#!/bin/bash

job="
use ronghui;
set mapreduce.job.queuename=root.ronghui.partner.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

alter table hgy_05 change imei app_package string;
alter table hgy_05 change score app_index int;
alter table hgy_05 add columns (app_freq int);
"
nohup beeline -e "$job" > log_modify_schema_05 &
