# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession, Window

import argparse

def getRawRecords(spark, fr, to):
	sql = """
		select
			*
		from
			ronghui.hgy_06
		where
			data_date between '{0}' and '{1}'
			and app_package is not null
			and app_package != ''
			and app_count > 0
	""".format(fr, to)
	print(sql)
	records = spark.sql(sql)
	return records

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_APP_Project___Prepare_Vertices') \
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

	print('====> Start calculation')
	records = getRawRecords(spark, args.fr, args.to)
	vertices = records.groupBy(['app_package']).agg(F.sum('app_count').alias('app_freq'))
	vertices = vertices.withColumn('partition', F.lit(1))
	vertices = vertices.withColumn('app_index', F.row_number().over(Window.partitionBy('partition').orderBy(F.col('app_freq'))))
	vertices = vertices.drop('partition').select('app_package', 'app_index', 'app_freq').registerTempTable('tmp')
	spark.sql('''INSERT OVERWRITE TABLE ronghui.hgy_05 PARTITION (data_date = '{0}') SELECT * FROM tmp'''.format(args.fr)).collect()
