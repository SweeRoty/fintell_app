# -*- coding: utf-8 -*-

from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def getAppsFromLBS(spark, fr, to, os):
	tbl = 'ronghui_mart.rh_lbs_wifi_daily' if os == 'a' else 'ronghui_mart.user_location_log_daily_ios'
	col = 'uid' if os == 'a' else 'md5(cast(uid as string))'
	sql = """
		select
			distinct {0} uid
		from
			{1}
		where
			data_date between '{2}' and '{3}'
			and from_unixtime(itime, 'yyyyMMdd') between '{2}' and '{3}'
	""".format(col, tbl, fr, to)
	print(sql)
	uids = spark.sql(sql)
	return uids

def getUIDs(spark, to, os):
	sql = """
		select
			distinct uid,
			package_name app_package
		from
			ronghui.register_user_log
		where
			data_date <= '{0}'
			and platform = '{1}'
	""".format(to, os)
	print(sql)
	uids = spark.sql(sql)
	return uids

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

def getOaidDevices(spark, data_date):
	sql = """
		select
			tmp.imei imei,
			max(tmp.imei_type) imei_type
		from
			(select
				imei,
				case when imei_type == 'imei' then 0 else 1 end imei_type
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

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../../stats/config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_APP_Project___Count_LBS_APPs') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext
	sc.setLogLevel('ERROR')

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--fr', type=str, help='Start date, format: yyyyMMdd')
	parser.add_argument('--to', type=str, help='End date, format: yyyyMMdd')
	parser.add_argument('--os', type=str, choices=['a', 'i'], default='a')
	parser.add_argument('--thres', type=int, default=2799)
	args = parser.parse_args()
	assert args.to >= args.fr

	print('====> Start computation')
	uids_lbs = getAppsFromLBS(spark, args.fr, args.to, args.os)
	uids = getUIDs(spark, args.to, args.os)
	uids_lbs = uids_lbs.join(uids, on='uid', how='inner')
	vertices = getVertices(spark, args.fr, args.thres)
	vertices = set(vertices.rdd.map(lambda row: row['app_package']).collect())
	uids_lbs = uids_lbs.where(uids_lbs.app_package.isin(vertices))
	lbs_app_count = uids_lbs.select('app_package').distinct().count()
	print('-----{0} APPs exist in LBS table'.format(lbs_app_count))