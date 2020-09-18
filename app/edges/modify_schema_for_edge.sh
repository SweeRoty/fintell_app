#!/bin/bash

job="
use ronghui;
set mapreduce.job.queuename=root.ronghui.partner.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

alter table hgy_07 change imei fr int;
alter table hgy_07 change score to int;
alter table hgy_07 add columns (weight float);
"
nohup beeline -e "$job" > log_modify_schema_07 &