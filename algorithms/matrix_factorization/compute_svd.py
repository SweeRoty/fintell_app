# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.linalg.distributed import RowMatrix
from pyspark.sql import SparkSession

import argparse

def double(row):
	return [(row['fr'], (row['to'], row['weight'])), (row['to'], (row['fr'], row['weight']))]

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('APP_Project___Compute_SVD') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	edges = spark.read.csv('/user/ronghui_safe/hgy/app/edges/202005_5days_freq_1e4_0.6', header=True)
	print edges.take(3)
	edges = edges.rdd.flatMap(double)
	print edges.take(3)