# -*- coding: utf-8 -*-

import argparse
import pandas as pd

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--v_file', type=str)
	parser.add_argument('--c_file', type=str)
	args = parser.parse_args()

	v = pd.read_csv(args.v_file)
	c = pd.read_csv(args.c_file)
	c.columns = ['app_index', 'community']
	v = v.merge(c, on='app_index', how='inner')

	print(v.loc[v.flag == 1].groupby('community').agg({'flag':'sum'}))