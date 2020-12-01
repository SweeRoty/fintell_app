#!/bin/bash

job="
use ronghui;
set mapreduce.job.queuename=root.ronghui.partner.preonline;
set hive.support.concurrency=false;
set hive.vectorized.execution=ture;

alter table hgy_08 change imei app_package string;
alter table hgy_08 change score removed_count int;
alter table hgy_08 add columns (installed_count int, removed_device_count int, installed_device_count int);
"
nohup beeline -e "$job" > log_modify_schema_08 &