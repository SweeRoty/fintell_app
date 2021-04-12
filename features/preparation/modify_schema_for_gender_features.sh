#!/bin/bash

job="
use ronghui;
set mapreduce.job.queuename=root.ronghui.partner.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

--alter table hgy_10 change imei app_package string;
--alter table hgy_10 change price_avg gender_avg float;
--alter table hgy_10 change price_std gender_na_ratio float;
alter table hgy_10 replace columns (app_package string, gender_avg float, gender_na_ratio float);
"
nohup beeline -e "$job" > log_modify_schema_10 &