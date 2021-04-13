# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
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

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_APP_Project___Clean_Vertices') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--data_date', type=str)
	parser.add_argument('--thres', type=str)
	args = parser.parse_args()

	print('====> Start computation')
	vertices = getVertices(spark, args.data_date, args.thres)
	vertices = vertices.withColumn('app_package', F.regexp_replace('app_package', '[^A-Za-z0-9_\\.]+', ''))
	# no starting with digits
	vertices = vertices.withColumn('app_package', F.regexp_replace('app_package', '\\.[0-9]+(?=\\.|$)', ''))
	vertices = vertices.withColumn('app_package', F.regexp_replace('app_package', '^com\\.', ''))
	vertices = vertices.withColumn('app_package', F.regexp_replace('app_package', '\\.com(?=\\.|$)', ''))
	vertices = vertices.withColumn('app_package', F.regexp_replace('app_package', '^cn\\.', ''))
	vertices = vertices.withColumn('app_package', F.regexp_replace('app_package', '\\.cn(?=\\.|$)', ''))
	vertices = vertices.withColumn('app_package', F.regexp_replace('app_package', '^www\\.', ''))
	vertices = vertices.withColumn('app_package', F.regexp_replace('app_package', '\\.www(?=\\.|$)', ''))
	vertices = vertices.withColumn('app_package', F.regexp_replace('app_package', '\\.', ''))
	vertices = vertices.repartition(100)
	# prefix or suffix?
	# print(vertices.where(vertices.app_package.rlike('^\\d+')).select('app_package').take(10))
	v = vertices.select(F.col('app_package').alias('app_package_l'), F.col('app_index').alias('app_index_l')) \
				.crossJoin(vertices.select(F.col('app_package').alias('app_package_r'), F.col('app_index').alias('app_index_r')))
	v = v.where(v.app_package_l < v.app_package_r)
	v = v.withColumn('len_l', F.length('app_package_l'))
	v = v.withColumn('len_r', F.length('app_package_r'))
	v = v.withColumn('levenshtein_l', F.levenshtein(F.col('app_package_l'), F.col('app_package_r')))
	v = v.withColumn('levenshtein_r', F.levenshtein(F.col('app_package_r'), F.col('app_package_l')))
	v = v.withColumn('levenshtein_l', F.col('levenshtein_l')/F.col('len_l'))
	v = v.withColumn('levenshtein_r', F.col('levenshtein_r')/F.col('len_r'))
	v = v.withColumn('levenshtein', F.greatest(F.col('levenshtein_l'), F.col('levenshtein_r'))).cache()
	v.select(['app_package_l', 'app_index_l', 'app_package_r', 'app_index_r', 'levenshtein']).where(F.col('levenshtein') <= 0.3) \
		.repartition(1) \
		.write \
		.csv('/user/ronghui_safe/hgy/app/vertices/levenshtein_dist_less_than_0.3_{0}_{1}'.format(args.data_date, args.thres))
