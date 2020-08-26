# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StringType

import argparse

def retrieveScannedDevices(spark, fr, to):
	sql = """
		select
			imei,
			count(distinct data_date) scanned_date_count
		from
			ronghui_mart.rh_stat_app_install_all
		where
			data_date between '{0}' and '{1}'
		group by
			imei
	""".format(fr, to)
	print(sql)
	scanned_devices = spark.sql(sql)
	return scanned_devices

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

def transform_to_row(t):
	app_package, status = t[0].split('sweeroty')
	return Row(app_package=app_package, status=int(status), count=int(t[1]))

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('../../stats/config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_APP_Project___Extract_Continuously_Scanned_Devices') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--fr', type=str)
	parser.add_argument('--to', type=str)
	args = parser.parse_args()
	query_month = args.fr[:6]
	month_end = str(monthrange(int(query_month[:4]), int(query_month[4:]))[1])
	month_end = query_month+month_end

	print('====> Start calculation')
	devices = retrieveScannedDevices(spark, args.fr, args.to)
	invalid_devices = getInvalidDevices(spark, month_end)
	devices = devices.join(invalid_devices, on='imei', how='left_outer').where(F.isnull(F.col('flag')))
	lasting_days = int(args.to)-int(args.fr)+1
	devices = devices.where(F.col('scanned_date_count') >= lasting_days)
	devices = devices.drop('scanned_date_count').drop('flag').withColumn('score', F.lit(None).cast(StringType()))
	devices.select('imei', 'score').registerTempTable('tmp')
	spark.sql('''INSERT OVERWRITE TABLE ronghui.hgy_01 PARTITION (data_date = '{0}') SELECT * FROM tmp'''.format(query_month)).collect()