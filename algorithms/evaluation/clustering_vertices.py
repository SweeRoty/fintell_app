# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
import argparse

from pyspark import SparkConf
from pyspark.ml.clustering import BisectingKMeans
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

def transform2vec(t):
	global n, max_dist
	items = sorted(t[1], key=lambda x: x[0])
	assert len(items) == n
	return Row(app_package=t[0], featureVec=Vectors.dense([max_dist-dist for _, dist in items]))

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_APP_Project___Cluster_Unknown_Vertices') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--is_training', default=False, action='store_true')
	args = parser.parse_args()

	print('====> Start computation')
	vertices = spark.read.csv('/user/ronghui_safe/hgy/app/gcn/top1_unknown_app_list_new.csv', header=True)
	n = vertices.count()
	vertices = vertices.withColumn('app_string', F.regexp_replace('app_package', '[^A-Za-z0-9_\\.]+', ''))
	# no starting with digits
	vertices = vertices.withColumn('app_string', F.regexp_replace('app_string', '\\.[0-9]+(?=\\.|$)', ''))
	vertices = vertices.withColumn('app_string', F.regexp_replace('app_string', '^com\\.', ''))
	vertices = vertices.withColumn('app_string', F.regexp_replace('app_string', '\\.com(?=\\.|$)', ''))
	vertices = vertices.withColumn('app_string', F.regexp_replace('app_string', '^cn\\.', ''))
	vertices = vertices.withColumn('app_string', F.regexp_replace('app_string', '\\.cn(?=\\.|$)', ''))
	vertices = vertices.withColumn('app_string', F.regexp_replace('app_string', '^www\\.', ''))
	vertices = vertices.withColumn('app_string', F.regexp_replace('app_string', '\\.www(?=\\.|$)', ''))
	vertices = vertices.withColumn('app_string', F.regexp_replace('app_string', '\\.', ''))
	vertices = vertices.repartition(100)
	# prefix or suffix?
	# print(vertices.where(vertices.app_string.rlike('^\\d+')).select('app_string').take(10))
	v = vertices.select(F.col('app_string').alias('app_string_l'), F.col('app_package').alias('app_package_l')) \
				.crossJoin(vertices.select(F.col('app_string').alias('app_string_r'), F.col('app_package').alias('app_package_r')))
	#v = v.where(v.app_string_l != v.app_string_r)
	v = v.withColumn('len_l', F.length('app_string_l'))
	v = v.withColumn('len_r', F.length('app_string_r'))
	v = v.withColumn('levenshtein_l', F.levenshtein(F.col('app_string_l'), F.col('app_string_r')))
	v = v.withColumn('levenshtein_r', F.levenshtein(F.col('app_string_r'), F.col('app_string_l')))
	v = v.withColumn('levenshtein_l', F.col('levenshtein_l')/F.col('len_l'))
	v = v.withColumn('levenshtein_r', F.col('levenshtein_r')/F.col('len_r'))
	if not args.is_training:
		v.select(['app_package_l', 'app_package_r', 'levenshtein_l']).repartition(1).write.csv('/user/ronghui_safe/hgy/app/gcn/top1_unknown_app_dist.csv', header=True)
	else:
		v = v.withColumn('levenshtein', F.greatest(F.col('levenshtein_l'), F.col('levenshtein_r'))).cache()
		max_dist = v.select(F.max('levenshtein').alias('max_levenshtein')).rdd.map(lambda row: row['max_levenshtein']).collect()[0] # 13
		v = v.rdd.map(lambda row: (row['app_package_l'], (row['app_package_r'], row['levenshtein']))).groupByKey().map(transform2vec).toDF()
		for K in [5, 10, 15, 20]:
			bkm = BisectingKMeans(featuresCol='featureVec', predictionCol='pred', maxIter=100, minDivisibleClusterSize=18).setK(K).setSeed(K*2)
			bkm_model = bkm.fit(v)
			print('====> The cost for K {} is {:.2f}'.format(K, bkm_model.computeCost(v)))
			bkm_model_summary = bkm_model.summary
			print('====> The size for each cluster size is {}'.format(bkm_model_summary.clusterSizes))
			print('-----')