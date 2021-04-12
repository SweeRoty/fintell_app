#!/bin/bash

job="
use ronghui;
set mapreduce.job.queuename=root.ronghui.partner.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

alter table hgy_03 change status degree_0_ratio float;
alter table hgy_03 change record_times degree_1_ratio float;
alter table hgy_03 change device_count degree_2_ratio float;
alter table hgy_03 add columns (degree_3_ratio float, degree_4_ratio float);
"
nohup beeline -e "$job" > log_modify_schema_03 &