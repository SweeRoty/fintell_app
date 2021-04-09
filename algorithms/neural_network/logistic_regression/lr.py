# -*- coding:utf-8 -*-

import argparse

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
import pandas as pd

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--is_testing', action='store_true', default=False, help='Testing mode or tuning mode')
	parser.add_argument('--C', type=int, default=0.01)
	args = parser.parse_args()

	"""
	app_nodes = pd.read_csv('../dl_data/app_nodes_2.csv')
	X_train = app_nodes.loc[app_nodes.phase == 0, ['removed_count', 'installed_count']].values
	y_train = app_nodes.label[app_nodes.phase == 0].values
	X_eval = app_nodes.loc[app_nodes.phase == 1, ['removed_count', 'installed_count']].values
	y_eval = app_nodes.label[app_nodes.phase == 1].values
	X_test = app_nodes.loc[app_nodes.phase == 2, ['removed_count', 'installed_count']].values
	y_test = app_nodes.label[app_nodes.phase == 2].values
	"""
	train = pd.read_csv('../mlp/app_data_train.csv')
	X_train = train.drop(['app_package', 'label'], axis=1).values
	y_train = train.label.values
	eval = pd.read_csv('../mlp/app_data_eval.csv')
	X_eval = eval.drop(['app_package', 'label'], axis=1).values
	y_eval = eval.label.values
	test = pd.read_csv('../mlp/app_data_test.csv')
	X_test = test.drop(['app_package', 'label'], axis=1).values
	y_test = test.label.values

	if not args.is_testing:
		for C in [0.0001, 0.001, 0.01, 0.1, 1, 10, 100]:
			lr = LogisticRegression(C=C, class_weight={0:1, 1:69})
			lr.fit(X_train, y_train)
			pred_train = lr.predict_proba(X_train)[:, 1]
			print('Training AUC for C {} is {:.4f}'.format(C, roc_auc_score(y_train, pred_train)))
			pred_eval = lr.predict_proba(X_eval)[:, 1]
			print('Validation AUC for C {} is {:.4f}'.format(C, roc_auc_score(y_eval, pred_eval)))
			print('\n')
	else:
		lr = LogisticRegression(C=args.C, class_weight={0:1, 1:69})
		lr.fit(X_train, y_train)
		pred_test = lr.predict_proba(X_test)[:, 1]
		print('Testing AUC under C {} is {:.4f}'.format(args.C, roc_auc_score(y_test, pred_test)))