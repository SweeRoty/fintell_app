# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse
import numpy as np

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

def loadSampledDevices(spark, query_month):
	sql = """
		select
			imei
		from
			ronghui.hgy_01
		where
			data_date = '{0}'
	""".format(query_month)
	print(sql)
	devices = spark.sql(sql)
	return devices

def getRawEdges(spark, fr, to):
	sql = """
		select
			distinct md5(cast(imei as string)) imei,
			package app_package
		from
			edw.app_list_install_uninstall_fact
		where
			data_date between '{0}' and '{1}'
			and status >= 1
			and imei is not null
			and imei != ''
			and package is not null
			and package != ''
	""".format(fr, to)
	print(sql)
	edges = spark.sql(sql)
	return edges

def getVertices(spark, query_month):
	sql = """
		select
			app_package,
			app_index,
			app_freq
		from
			ronghui.hgy_05
		where
			data_date = '{0}'
	""".format(query_month)
	print(sql)
	vertices = spark.sql(sql)
	return vertices

def generateEdge(t):
	import random, time
	apps = sorted(t[1], key=lambda t: t[0])
	n = len(apps)
	edges = []
	seed = int(time.time())%202005
	count = 0
	if n > 1:
		for i in range(n-1):
			for j in range(i+1, n):
				#if count > 9:
				#	break
				#edge_prob = 1e7/(apps[i][0]*apps[j][0])
				edge_prob = 1.1e4/(np.power(apps[i][1], 0.6)*np.power(apps[j][1], 0.6))
				#edges.append(('{0}_{1}'.format(apps[i][0], apps[j][0]), edge_prob))
				random.seed(seed+count)
				if random.random() < edge_prob:
					edge_name = '{0}sweeroty{1}'.format(apps[i][0], apps[j][0])
					edges.append(edge_name)
				count += 1
	return edges

def transform_to_row(t):
	fr, to = t[0].split('sweeroty')
	return Row(fr=fr, to=to, weight=int(t[1]))

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_APP_Project___Extract_Edges') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--fr', type=str)
	parser.add_argument('--to', type=str)
	parser.add_argument('--device_date', type=str)
	args = parser.parse_args()
	query_month = args.fr[:6]
	month_end = query_month+str(monthrange(int(query_month[:4]), int(query_month[4:]))[1])

	print('====> Start calculation')
	edges = getRawEdges(spark, args.fr, args.to)
	"""
	devices = getInvalidDevices(spark, month_end)
	edges = edges.join(devices, on=['imei'], how='left_outer').where(F.col('flag').isNull())
	"""
	devices = loadSampledDevices(spark, args.device_date).sample(False, 0.05, 11267)
	edges = edges.join(devices, on=['imei'], how='inner')
	vertices = getVertices(spark, args.fr)
	vertices = vertices.where(vertices.app_freq > 27000)
	edges = edges.join(vertices, on=['app_package'], how='inner').drop('app_package')
	#edges = edges.where(edges.app_freq > 27000)
	edges = edges.repartition(30000, 'imei').rdd.map(lambda row: (row['imei'], (row['app_index']-17477003, row['app_freq']-27000))).groupByKey().flatMap(generateEdge)
	edges = edges.map(lambda t: (t, 1)).reduceByKey(lambda x, y: x+y).map(transform_to_row).toDF()
	edges = edges.select('fr', 'to', 'weight').registerTempTable('tmp')
	spark.sql('''INSERT OVERWRITE TABLE ronghui.hgy_07 PARTITION (data_date = '{0}') SELECT * FROM tmp'''.format(args.fr)).collect()
	#edges.repartition(1).write.csv('/user/hive/warehouse/ronghui_bj.db/hgy/app/edges/{0}_7days_freq_11000_0.6'.format(args.fr), header=True)