# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
from operator import add
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def getVertices(spark, data_date, thres):
	sql = """
		select
			*
		from
			ronghui.hgy_05
		where
			data_date = '{0}'
			and app_freq > {1}
	""".format(data_date, thres)
	print(sql)
	vertices = spark.sql(sql)
	return vertices

def getAppCategories(spark):
	sql = """
		select
			distinct package app_package,
			class_one,
			class_two,
			class_three
		from
			ronghui_mart.app_info
		where
			(class_one is not null and class_one not in ('N', 'NULL', ''))
			or (class_two is not null and class_two not in ('N', 'NULL', ''))
			or (class_three is not null and class_three not in ('N', 'NULL', ''))
	"""
	apps = spark.sql(sql)
	return apps

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_APP_Project___Get_Labels_of_Vertices') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--data_date', type=str)
	parser.add_argument('--thres', type=int)
	parser.add_argument('--verbose', action='store_true')
	args = parser.parse_args()

	print('====> Start computation')
	vertices = getVertices(spark, args.data_date, args.thres)
	labels = getAppCategories(spark)
	vertices = vertices.join(labels, on='app_package', how='left_outer').drop('data_date')
	apps_debt = spark.read.csv('/user/ronghui_safe/hgy/app/manual_labeling/debt_app.csv', sep='\t', header=True)
	apps_debt = apps_debt.select('app_package').distinct().rdd.map(lambda row: row['app_package']).collect()
	vertices = vertices.withColumn('flag_debt', F.when(vertices.app_package.isin(apps_debt), 1).otherwise(0))
	apps_714 = spark.read.csv('/user/ronghui_safe/hgy/app/manual_labeling/714.csv', sep='\t', header=True)
	apps_714 = apps_714.select('app_package').distinct().rdd.map(lambda row: row['app_package']).collect()
	vertices = vertices.withColumn('flag_714', F.when(vertices.app_package.isin(apps_714), 1).otherwise(0))
	if args.verbose:
		class_stats = vertices.rdd.map(lambda row: (row['class_one'].encode('utf-8') if row['class_one'] is not None else 'NA', 1)).reduceByKey(add).collect()
		for cl, count in class_stats:
			print("-----Category {0}'s count is {1}".format(cl, count))
	vertices.repartition(1).write.csv('/user/ronghui_safe/hgy/app/samples/samples_with_label_{0}_{1}'.format(args.data_date, args.thres), header=True)