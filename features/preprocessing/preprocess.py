# -*- coding: utf-8 -*-

import argparse

import numpy as np
import pandas as pd

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--input', type=str)
	parser.add_argument('--output', type=str)
	args = parser.parse_args()

	features = pd.read_csv(args.input)

	"""
	## price
	min_price_avg = features.price_avg.min()
	features.price_avg.fillna(min_price_avg, inplace=True)
	max_price_avg = features.price_avg.max()
	features.loc[:, 'price_avg'] = (features.loc[:, 'price_avg'] - min_price_avg) / (max_price_avg - min_price_avg)
	features.price_std.fillna(0, inplace=True)
	max_price_std = features.price_std.max()
	features.loc[:, 'price_std'] = features.loc[:, 'price_std'] / max_price_std
	features.price_na_ratio.fillna(1, inplace=True)

	## OAID
	features.oaid_avg.fillna(0, inplace=True)
	features.oaid_na_ratio.fillna(1, inplace=True)

	## gender
	features.gender_avg.fillna(0.5, inplace=True)
	features.gender_na_ratio.fillna(1, inplace=True)

	## age
	age_cols = ['age_{0}_ratio'.format(i) for i in range(5)]
	age_sums = features.loc[:, age_cols].sum(axis=1)
	age_sums.replace(0, 1, inplace=True)
	for col in age_cols:
		features.loc[:, col] = features.loc[:, col]/age_sums
		features.loc[:, col].fillna(0, inplace=True)
	del age_sums

	## degree
	degree_cols = ['degree_{0}_ratio'.format(i) for i in range(5)]
	degree_sums = features.loc[:, degree_cols].sum(axis=1)
	degree_sums.replace(0, 1, inplace=True)
	for col in degree_cols:
		features.loc[:, col] = features.loc[:, col]/degree_sums
		features.loc[:, col].fillna(0, inplace=True)
	del degree_sums
	"""

	## installment
	max_removed_count = features.removed_count.max()
	features.loc[:, 'removed_count'] = np.log(features.loc[:, 'removed_count'].values+1)/np.log(max_removed_count+1)
	max_installed_count = features.installed_count.max()
	features.loc[:, 'installed_count'] = np.log(features.loc[:, 'installed_count'].values+1)/np.log(max_installed_count+1)
	max_removed_device_count = features.removed_device_count.max()
	features.loc[:, 'removed_device_count'] = np.log(features.loc[:, 'removed_device_count'].values+1)/np.log(max_removed_device_count+1)
	max_installed_device_count = features.installed_device_count.max()
	features.loc[:, 'installed_device_count'] = np.log(features.loc[:, 'installed_device_count'].values+1)/np.log(max_installed_device_count+1)
	for col in ['removed_count', 'installed_count', 'removed_device_count', 'installed_device_count']:
		features.loc[:, col].fillna(0, inplace=True)

	print(features.describe())
	"""
	features.drop(['oaid_avg', 'gender_na_ratio', 'removed_device_count', 'installed_device_count'], axis=1, inplace=True)
	"""
	features.drop(['removed_device_count', 'installed_device_count'], axis=1).to_csv(args.output, index=False)