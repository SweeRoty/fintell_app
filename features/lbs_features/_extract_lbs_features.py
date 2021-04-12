# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
from datetime import datetime
from operator import add
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def getVertices(spark, data_date, freq_thres):
	sql = """
		select
			app_package
		from
			ronghui.hgy_05
		where
			data_date = '{0}'
			and app_freq > '{1}'
	""".format(data_date, freq_thres)
	print(sql)
	vertices = spark.sql(sql)
	return vertices

def getUids(spark, to, os):
	device = 'imei' if os == 'a' else 'idfa'
	sql = """
		select
			distinct uid,
			package_name app_package,
			{0} device_id
		from
			ronghui.register_user_log
		where
			data_date <= '{1}'
			and platform = '{2}'
	""".format(device, to, os)
	print(sql)
	uids = spark.sql(sql)
	return uids

def getLBSPoints(spark, fr, to, os):
	tbl = 'ronghui_mart.rh_lbs_wifi_daily' if os == 'a' else 'ronghui_mart.user_location_log_daily_ios'
	col = 'uid' if os == 'a' else 'md5(cast(uid as string))'
	sql = """
		select
			distinct {0} uid,
			itime,
			coordinate,
			coordinate_source source
		from
			{1}
		where
			data_date between '{2}' and '{3}'
			and from_unixtime(itime, 'yyyyMMdd') between '{2}' and '{3}'
	""".format(col, tbl, fr, to)
	print(sql)
	uids = spark.sql(sql)
	return uids

def transform_to_row(t):
	app_package, status = t[0].split('_rlab_')
	return Row(app_package=app_package, status=status, count=int(t[1]))

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_APP_Project___Extract_LBS_Features') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--freq_thres', type=int)
	parser.add_argument('--fr', type=str, help='Start date, format: yyyyMMdd')
	parser.add_argument('--to', type=str, help='End date, format: yyyyMMdd')
	parser.add_argument('--os', choices=['a', 'i'])
	args = parser.parse_args()
	assert args.to >= args.fr
	n = (datetime.strptime(args.to, "%Y%m%d") - datetime.strptime(args.fr, "%Y%m%d")).days+1

	print('====> Start computation')
	vertices = getVertices(spark, args.fr, args.freq_thres)
	vertices = set(vertices.rdd.map(lambda row: row['app_package']).collect())
	uids = getUids(spark, args.to, args.os)
	uids = uids.where(uids.app_package.isin(vertices))
	points = getLBSPoints(spark, args.fr, args.to, args.os)
	points = points.join(uids, on='uid', how='inner').cache()
	del vertices
	### to be done