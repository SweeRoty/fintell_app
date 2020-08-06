# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession, Window

import argparse

def transform_to_row(row_dict):
	global args
	row_dict['data_date'] = args.query_month
	return Row(**row_dict)

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Cal_APP_Installment_Stats') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str)
	args = parser.parse_args()
	fr = args.query_month+'01'
	to = args.query_month+str(monthrange(int(args.query_month[:4]), int(args.query_month[4:]))[1])

	print('====> Start calculation')
	app = spark.read.csv('/user/hive/warehouse/ronghui.db/rlab_stats_report/installment/app/{0}'.format(args.query_month), header=True)
	app_stats = app.groupby('status').agg(F.mean('count').alias('avg_device_count')).collect()
	print(app_stats)
	app = app.withColumn('app_index', F.row_number().over(Window.partitionBy('status').orderBy(F.col('count').desc())))
	app = app.select('app_package', 'status', 'app_index', F.col('count').alias('app_count'))
	app.repartition(1).write.csv('/user/ronghui_safe/hgy/rlab_stats_report/installment/app_rank/{0}'.format(args.query_month), header=True)