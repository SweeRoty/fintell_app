# -*- coding: utf-8 -*-

from sklearn.cluster import AgglomerativeClustering
import numpy as np
import pandas as pd

if __name__ == '__main__':
	dist = pd.read_csv('./top1_unknown_app_dist.csv')
	apps = pd.DataFrame({'app':dist.app_package_l.unique()})
	n = apps.shape[0]
	apps['app_index'] = np.arange(n)
	dist = dist.merge(apps, left_on='app_package_l', right_on='app', how='inner')
	dist = dist.merge(apps, left_on='app_package_r', right_on='app', how='inner')
	dist.drop(['app_x', 'app_y'], axis=1, inplace=True)
	dist_mat = np.zeros((n, n))
	dist_mat[dist.app_index_x.values, dist.app_index_y.values] = dist.levenshtein_l.values

	hcluster = AgglomerativeClustering(n_clusters=10, affinity='precomputed', linkage='complete')
	apps['label'] = hcluster.fit_predict(dist_mat)
	stats = apps.groupby('label').agg({'app':'count'})
	for lbl in range(10):
		print('====> The size of cluster {} is {}'.format(lbl, stats.loc[lbl].app))
		print apps.loc[apps.label == lbl].head(n=10)
		print('-----')