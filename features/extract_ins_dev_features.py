# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
from datetime import datetime, timedelta
from operator import add
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def getVertices(spark, data_date, thres):
	sql = """
		select
			app_package
		from
			ronghui.hgy_05
		where
			data_date = '{0}'
			and app_freq > '{1}'
	""".format(data_date, thres)
	print(sql)
	vertices = spark.sql(sql)
	return vertices

def retrieveRawRecords(spark, fr, to):
	sql = """
		select
			distinct md5(cast(imei as string)) imei,
			package app_package,
			status
		from
			edw.app_list_install_uninstall_fact
		where
			data_date between '{0}' and '{1}'
			and from_unixtime(last_report_time, 'yyyyMMdd') = data_date
			and status != 1
			and imei is not null
			and imei != ''
			and package is not null
			and package != ''
	""".format(fr, to)
	print(sql)
	records = spark.sql(sql)
	return records

def loadSampledDevices(spark, data_date):
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

def getProperties(spark, data_date):
	sql = """
		select
			imei,
			--model,
			brand,
			price
		from
			ronghui_mart.rt_device_prop
		where
			data_date = '{0}'
			and platform = 'Android'
	""".format(data_date)
	print(sql)
	props = spark.sql(sql)
	return props

def getOaidDevices(spark, data_date):
	sql = """
		select
			tmp.imei imei,
			max(tmp.imei_type) imei_type
		from
			(select
				imei,
				case when imei_type = 'imei' then 0 else 1 end imei_type
			from
				ronghui_mart.imei_oaid_fact
			where
				data_date = '{0}'
			) as tmp
		group by
			tmp.imei
	""".format(data_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def getPortraits(spark, data_date, kind):
	assert kind in ['nl', 'xb', 'xl']
	col = 'gender' if kind == 'xb' else kind
	portrait_dict = {'nl':'age', 'xb':'gender', 'xl':'degree'}
	sql = """
		select
			imei,
			{0} {1}
		from
			ronghui_mart.rh_xxd_{2}_model_v1
		where
			data_date = '{3}'
	""".format(col, portrait_dict[kind], kind, data_date)
	print(sql)
	portraits = spark.sql(sql)
	return portraits

def transform_to_row(t):
	app_package, status = t[0].split('_rlab_')
	return Row(app_package=app_package, status=status, count=int(t[1]))

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('./config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_APP_Project___Extract_Installment_Features') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--thres', type=int)
	parser.add_argument('--fr', type=str, help='Start date, format: yyyyMMdd')
	parser.add_argument('--to', type=str, help='End date, format: yyyyMMdd')
	args = parser.parse_args()
	assert args.to >= args.fr
	month_end = datetime.strptime(args.fr, '%Y%m%d').replace(day=1)-timedelta(days=1)
	month_end = month_end.strftime('%Y%m%d')

	print('====> Start computation')
	vertices = getVertices(spark, args.fr, args.thres)
	vertices = set(vertices.rdd.map(lambda row: row['app_package'].encode('utf-8')).collect())

	records = retrieveRawRecords(spark, args.fr, args.to)
	records = records.rdd.filter(lambda row: row['app_package'].encode('utf-8') in vertices).toDF()

	"""
		Step 2: Computing APP's device features based on their installed devices
	"""
	devices = loadSampledDevices(spark, args.fr).sample(False, 0.05, 11267)
	props = getProperties(spark, month_end)
	devices = devices.join(props, on='imei', how='left_outer')
	#oaids = getOaidDevices(spark, args.to)
	#devices = devices.join(oaids, on='imei', how='left_outer')
	for kind in ['nl', 'xb', 'xl']:
		portraits = getPortraits(spark, month_end, kind)
		devices = devices.join(portraits, on='imei', how='left_outer')
	records = records.join(devices, on='imei', how='inner')

	vertices = [(app_package,) for app_package in vertices]
	vertices = spark.createDataFrame(data=vertices, schema=['app_package'])

	dev_props_1 = records.groupBy('app_package') \
							.agg(F.mean('price').alias('price_avg'), \
								F.stddev('price').alias('price_std'), \
								F.mean(F.when(F.isnull('price'), 1).otherwise(0)).alias('price_na_ratio'), \
								#F.mean('imei_type').alias('oaid_avg'), \
								#F.mean(F.when(F.isnull('imei_type'), 1).otherwise(0)).alias('oaid_na_ratio'), \
								F.mean('gender').alias('gender_avg'), \
								F.mean(F.when(F.isnull('gender'), 0).otherwise(1)).alias('gender_na_ratio'))
	vertices = vertices.join(dev_props_1, on='app_package', how='left_outer')

	dev_props_2 = records.groupBy('app_package') \
							.pivot('age', [0, 1, 2, 3, 4]) \
							.agg(F.count('imei'))
	cols = dev_props_2.columns
	cols = [col if col == 'app_package' else 'age_{0}_ratio'.format(col) for col in cols]
	dev_props_2 = dev_props_2.toDF(*cols)
	for age in range(5):
		col = 'age_{0}_ratio'.format(age)
		dev_props_2 = dev_props_2.withColumn(col, F.when(dev_props_2[col].isNull(), F.lit(0)).otherwise(dev_props_2[col]))
	vertices = vertices.join(dev_props_2, on='app_package', how='left_outer')

	dev_props_3 = records.groupBy('app_package') \
							.pivot('degree', [0, 1, 2, 3, 4]) \
							.agg(F.count('imei'))
	cols = dev_props_3.columns
	cols = [col if col == 'app_package' else 'degree_{0}_ratio'.format(col) for col in cols]
	dev_props_3 = dev_props_3.toDF(*cols)
	for degree in range(5):
		col = 'degree_{0}_ratio'.format(degree)
		dev_props_3 = dev_props_3.withColumn(col, F.when(dev_props_3[col].isNull(), F.lit(0)).otherwise(dev_props_3[col]))
	vertices = vertices.join(dev_props_3, on='app_package', how='left_outer')

	vertices = vertices.select('app_package', 'price_avg', 'price_std', 'price_na_ratio', 'gender_avg', 'gender_na_ratio' \
								'age_0_ratio', 'age_1_ratio', 'age_2_ratio', 'age_3_ratio', 'age_4_ratio' \
								'degree_0_ratio', 'degree_1_ratio', 'degree_2_ratio', 'degree_3_ratio', 'degree_4_ratio') \
								.registerTempTable('tmp')
	spark.sql('''INSERT OVERWRITE TABLE ronghui.hgy_09 PARTITION (data_date = '{0}') SELECT * FROM tmp'''.format(args.fr)).collect()

	"""
		Step 3: Computing APP's installment count features
	"""
	status = ['0', '2']
	status_dict = {'0':'removed', '2':'installed'}
	records = records.select(['app_package', 'status', 'imei']).cache()
	vertices = vertices.select(['app_package']).cache()

	counts = records.rdd \
					.map(lambda row: ('{0}_rlab_{1}'.format(row['app_package'].encode('utf-8'), row['status']), 1)) \
					.reduceByKey(add) \
					.map(transform_to_row) \
					.toDF() \
					.groupBy('app_package') \
					.pivot('status', status) \
					.sum()
	cols = counts.columns
	cols = [col if col == 'app_package' else '{0}_count'.format(status_dict[col]) for col in cols]
	counts = counts.toDF(*cols)
	vertices = vertices.join(counts, on='app_package', how='left_outer')

	dev_counts = records.rdd \
						.map(lambda row: ('{0}_rlab_{1}'.format(row['app_package'].encode('utf-8'), row['status']), row['imei'])) \
						.distinct() \
						.map(lambda t: (t[0], 1)) \
						.reduceByKey(add) \
						.map(transform_to_row) \
						.toDF() \
						.groupBy('app_package') \
						.pivot('status', status) \
						.sum()
	cols = dev_counts.columns
	cols = [col if col == 'app_package' else '{0}_device_count'.format(status_dict[col]) for col in cols]
	dev_counts = dev_counts.toDF(*cols)
	vertices = vertices.join(dev_counts, on='app_package', how='left_outer')

	vertices = vertices.select('app_package', 'removed_count', 'installed_count', 'removed_device_count', 'installed_device_count').registerTempTable('tmp')
	spark.sql('''INSERT OVERWRITE TABLE ronghui.hgy_08 PARTITION (data_date = '{0}') SELECT * FROM tmp'''.format(args.fr)).collect()