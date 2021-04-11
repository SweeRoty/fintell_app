# -*- coding: utf-8 -*-

import argparse

import numpy as np
import pandas as pd

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--n_path', type=str, default='./app_raw_nodes.csv', help='File path of raw nodes')
	parser.add_argument('--sample', type=str, default='./final_samples.csv', help='File path of samples')
	parser.add_argument('--e_path', type=str, default='./app_raw_edges.csv', help='File path of raw edges')
	args = parser.parse_args()

	x = pd.read_csv(args.n_path)
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

	for col in x.columns:
		if 'count' in col:
			col_max = x[col].max()
			x.loc[:, col] = np.log(x.loc[:, col]+1)/np.log(col_max+1)

	samples = pd.read_csv(args.sample)
	samples = x.merge(samples, on='app_package', how='inner')
	samples.sort_values('app_index', ascending=True, inplace=True)
	samples.drop('app_index', axis=1).to_csv('./app_nodes.csv', index=False)

	edges = pd.read_csv(args.e_path)
	edges.loc[:, 'fr'] = edges.loc[:, 'fr'] - 1
	edges.loc[:, 'to'] = edges.loc[:, 'to'] - 1
	edges.drop('data_date', axis=1).to_csv('./app_edges.csv', index=False)