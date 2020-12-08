# -*- coding: utf-8 -*-

import argparse

import pandas as pd

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--sample_file', type=str)
	parser.add_argument('--feature_file', type=str)
	parser.add_argument('--node_output', type=str)
	parser.add_argument('--edge_file', type=str)
	parser.add_argument('--edge_output', type=str)
	args = parser.parse_args()

	samples = pd.read_csv(args.sample_file)
	features = pd.read_csv(args.feature_file)
	samples = features.merge(samples, on='app_package', how='inner')
	samples.sort_values('app_index', ascending=True, inplace=True)
	samples.drop('app_index', axis=1).to_csv(args.node_output, index=False)

	edges = pd.read_csv(args.edge_file)
	edges.loc[:, 'fr'] = edges.loc[:, 'fr'] - 1
	edges.loc[:, 'to'] = edges.loc[:, 'to'] - 1
	edges.drop('data_date', axis=1).to_csv(args.node_output, index=False)