#!/bin/bash

job="
use ronghui;
set mapreduce.job.queuename=root.ronghui.partner.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

alter table hgy_06 change imei app_package string;
alter table hgy_06 change score app_count int;
"
nohup beeline -e "$job" > log_modify_schema_06 &
