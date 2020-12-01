#!/bin/bash

job="
use ronghui;
set mapreduce.job.queuename=root.ronghui.partner.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

alter table hgy_09 change imei app_package string;
alter table hgy_09 change score price_avg float;
alter table hgy_09 add columns (price_std float, price_na_ratio float, gender_avg float, gender_na_ratio float, 
age_0_ratio float, age_1_ratio float, age_2_ratio float, age_3_ratio float, age_4_ratio float, 
degree_0_ratio float, degree_1_ratio float, degree_2_ratio float, degree_3_ratio float, degree_4_ratio float);
"
nohup beeline -e "$job" > log_modify_schema_09 &