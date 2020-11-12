# -*- coding: utf-8 -*-

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

def loadSampledDevices(spark, device_date):
	sql = """
		select
			imei
		from
			ronghui.hgy_01
		where
			data_date = '{0}'
	""".format(device_date)
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

def getVertices(spark, data_date):
	sql = """
		select
			app_package,
			app_index,
			app_freq
		from
			ronghui.hgy_05
		where
			data_date = '{0}'
	""".format(data_date)
	print(sql)
	vertices = spark.sql(sql)
	return vertices

def generateEdge(t):
	global gamma, penalty, seed
	import random, time
	apps = sorted(t[1], key=lambda t: t[0])
	n = len(apps)
	edges = []
	seed = int(time.time())%seed
	count = 0
	if n > 1:
		for i in range(n-1):
			for j in range(i+1, n):
				edge_prob = gamma/(np.power(apps[i][1], penalty)*np.power(apps[j][1], penalty))
				random.seed(seed+count)
				if random.random() < edge_prob:
					edge_name = '{0}_rlab_{1}'.format(apps[i][0], apps[j][0])
					edges.append(edge_name)
				count += 1
	return edges

def transform_to_row(t):
	fr, to = t[0].split('_rlab_')
	return Row(fr=fr, to=to, weight=int(t[1]))

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../config')
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
	parser.add_argument('--freq_thres', type=int)
	parser.add_argument('--gamma', type=float, default=7.5e3)
	parser.add_argument('--penalty', type=float, default=0.75)
	parser.add_argument('--seed', type=int, default=198910)
	args = parser.parse_args()
	gamma = args.gamma
	penalty = args.penalty
	seed = args.seed

	print('====> Start computation')
	edges = getRawEdges(spark, args.fr, args.to)
	devices = loadSampledDevices(spark, args.fr).sample(False, 0.05, 11267)
	edges = edges.join(devices, on=['imei'], how='inner')
	vertices = getVertices(spark, args.fr)
	index_thres = vertices.where(F.col('app_freq') <= args.freq_thres) \
							.select(F.max('app_index').alias('index_thres')) \
							.rdd \
							.map(lambda row: row['index_thres']) \
							.collect()[0]
	vertices = vertices.where(F.col('app_freq') > args.freq_thres)
	edges = edges.join(vertices, on=['app_package'], how='inner').drop('app_package')
	edges = edges.repartition(30000, 'imei') \
					.rdd \
					.map(lambda row: (row['imei'], (row['app_index']-index_thres, row['app_freq']-args.freq_thres))) \
					.groupByKey() \
					.flatMap(generateEdge)
	edges = edges.map(lambda t: (t, 1)).reduceByKey(lambda x, y: x+y).map(transform_to_row).toDF()
	edges = edges.select('fr', 'to', 'weight').registerTempTable('tmp')
	spark.sql('''INSERT OVERWRITE TABLE ronghui.hgy_07 PARTITION (data_date = '{0}') SELECT * FROM tmp'''.format(args.fr)).collect()