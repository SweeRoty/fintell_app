#!/bin/bash

job="
use ronghui;
set mapreduce.job.queuename=root.ronghui.partner.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

alter table hgy_02 change status app_package string;
alter table hgy_02 change avg_action_per_device age_0_ratio float;
alter table hgy_02 change avg_app_per_device age_1_ratio float;
alter table hgy_02 add columns (age_2_ratio float, age_3_ratio float, age_4_ratio float);
"
nohup beeline -e "$job" > log_modify_schema_02 &