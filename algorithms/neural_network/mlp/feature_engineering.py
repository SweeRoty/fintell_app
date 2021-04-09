# -*- coding: utf-8 -*-

import argparse

import numpy as np
import pandas as pd

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--sample', type=str, default='./final_samples.csv', help='File path of samples')
	parser.add_argument('--x_path', type=str, default='./app_features.csv', help='File path of features')
	args = parser.parse_args()

	x = pd.read_csv(args.x_path)
	x.drop(['removed_device_count', 'installed_device_count'], axis=1, inplace=True)
	for ins_col in ['removed_count', 'installed_count']:
		x.loc[x[ins_col].isna(), ins_col] = 0
	x.loc[x.gender_na_ratio.isna(), 'gender_na_ratio'] = 1
	gender_avg_mean = np.mean(x.gender_avg)
	x.loc[x.gender_avg.isna(), 'gender_avg'] = gender_avg_mean
	for kind in ['degree', 'age']:
		for i in range(5):
			indices = x['{}_{}_ratio'.format(kind, i)].isna()
			x.loc[indices, '{}_{}_ratio'.format(kind, i)] = 0
		x['{}_count'.format(kind)] = x['{}_0_ratio'.format(kind)] + x['{}_1_ratio'.format(kind)] + x['{}_2_ratio'.format(kind)] + x['{}_3_ratio'.format(kind)] + x['{}_4_ratio'.format(kind)]
		for i in range(5):
			x['{}_{}_count'.format(kind, i)] = x.loc[:, '{}_{}_ratio'.format(kind, i)]
			x['{}_{}_ratio'.format(kind, i)] = 0
			indices = x['{}_count'.format(kind)] != 0
			x.loc[indices, '{}_{}_ratio'.format(kind, i)] = np.around(x.loc[indices, '{}_{}_count'.format(kind, i)]/x.loc[indices, '{}_count'.format(kind)], 4)
	x.drop(['degree_4_count', 'degree_4_ratio'], axis=1, inplace=True)

	samples = pd.read_csv(args.sample)
	samples = samples.loc[samples.phase > -1, ['app_package', 'phase', 'label']]
	x = x.merge(samples, on='app_package', how='inner')
	x_train = x.loc[x.phase == 0]
	x_valid = x.loc[x.phase == 1]
	x_test = x.loc[x.phase == 2]
	for col in x_train.columns:
		if 'count' in col:
			col_max = x_train[col].max()
			x_train.loc[:, col] = np.log(x_train.loc[:, col]+1)/np.log(col_max+1)
			x_valid.loc[:, col] = np.log(x_valid.loc[:, col]+1)/np.log(col_max+1)
			x_test.loc[:, col] = np.log(x_test.loc[:, col]+1)/np.log(col_max+1)
	x_train.drop(['phase'], axis=1).to_csv('app_data_train.csv', index=False, header=True)
	x_valid.drop(['phase'], axis=1).to_csv('app_data_valid.csv', index=False, header=True)
	x_test.drop(['phase'], axis=1).to_csv('app_data_test.csv', index=False, header=True)