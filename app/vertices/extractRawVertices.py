# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def getInvalidDevices(spark, data_date):
	sql = """
		select
			imei,
			1 flag
		from
			ronghui_mart.sz_device_list
		where
			data_date = '{0}'
	""".format(data_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def getValidDevices(spark, data_date):
	sql = """
		select
			imei
		from
			ronghui.hgy_01
		where
			data_date = '{0}'
	""".format(data_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def getRawRecords(spark, data_date):
	sql = """
		select
			distinct md5(cast(imei as string)) imei,
			package app_package
		from
			edw.app_list_install_uninstall_fact
		where
			data_date = '{0}'
			and status >= 1
			and imei is not null
			and imei != ''
			and package is not null
			and package != ''
	""".format(data_date)
	print(sql)
	records = spark.sql(sql)
	return records

def transform_to_row(t):
	return Row(app_package=t[0], app_count=int(t[1]))

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_APP_Project___Extract_Raw_Vertices') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_date', type=str)
	parser.add_argument('--data_date', type=str)
	args = parser.parse_args()
	query_month = args.query_date[:6]
	month_end = query_month+str(monthrange(int(query_month[:4]), int(query_month[4:]))[1])

	print('====> Start calculation')
	'''
	devices = getInvalidDevices(spark, month_end)
	records = getRawRecords(spark, args.query_date)
	records = records.join(devices, on=['imei'], how='left_outer').where(F.col('flag').isNull())
	'''
	devices = getValidDevices(spark, args.data_date)
	records = getRawRecords(spark, args.query_date)
	records = records.join(devices, on=['imei'], how='inner')
	apps = records.drop('imei').repartition(10000, 'app_package').rdd.map(lambda row: (row['app_package'].encode('utf-8'), 1)).reduceByKey(lambda x, y: x+y)
	apps = apps.map(transform_to_row).toDF().select('app_package', 'app_count').registerTempTable('tmp')
	spark.sql('''INSERT OVERWRITE TABLE ronghui.hgy_06 PARTITION (data_date = '{0}') SELECT * FROM tmp'''.format(args.query_date)).collect()