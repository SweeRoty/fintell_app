# -*- coding: utf-8 -*-

import argparse

from sklearn.metrics import roc_auc_score
import numpy as np
import pandas as pd

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--pred', type=str, default='../abcde/app_gcn_layer8-8_v2_prd.npy', help='Prediction of GCN.')
	parser.add_argument('--sample_file', type=str, default='../dl_data/final_samples_0901_2799.csv', help='Sample file.')
	args = parser.parse_args()

	y_hat = np.load(args.pred)
	final_samples = pd.read_csv(args.sample_file)
	#raw_samples = pd.read_csv('../dl_data/samples_0901_2799.csv', usecols=['app_index', 'class_one', 'class_two', 'class_three'])
	#raw_samples.loc[:, 'app_index'] = raw_samples.loc[:, 'app_index']-21373006

	final_samples['pred_0'] = y_hat[:, 0]
	final_samples['pred_1'] = y_hat[:, 1]
	#final_samples = final_samples.merge(raw_samples, on='app_index', how='inner')
	print('====> The shape of the whole dataset is {}'.format(final_samples.shape))

	for phase in [0, 1, 2]:
		indices = final_samples.phase == phase
		print('\tAUC of Phase {:d} is {:.6f}'.format(phase, roc_auc_score(final_samples.loc[indices, 'label'], final_samples.loc[indices, 'pred_1'])))
	print('-----')

	s = final_samples.sort_values('pred_1', ascending=False)
	s = s.loc[s.phase != -1]
	s['perc'] = np.arange(s.shape[0])/s.shape[0]
	n_pos = s.label.sum()
	for thres in [0.01, 0.02, 0.05, 0.1]:
		n_recalled = s.label[s.perc <= thres].sum()
		print('\tRecall for Top {:.0f}% is {:.4f}'.format(thres*100, n_recalled/n_pos))
	print('-----')

	s = final_samples.sort_values('pred_1', ascending=False)
	s['perc'] = np.arange(s.shape[0])/s.shape[0]
	n_pos = s.label.sum()
	for thres in [0.01, 0.02, 0.05, 0.1]:
		n_recalled = s.label[s.perc <= thres].sum()
		print('\tRecall for Top {:.0f}% is {:.4f}'.format(thres*100, n_recalled/n_pos))
	print('-----')

	s = final_samples.loc[final_samples.phase == -1]
	print('====> The shape of the unlabeled dataset is {}'.format(s.shape))
	s = s.sort_values('pred_1', ascending=False)
	s['perc'] = np.arange(s.shape[0])/s.shape[0]
	n_class_one = np.sum(s.class_one == '金融理财')
	for thres in [0.01, 0.02, 0.05, 0.1]:
		n_recalled = np.sum((s.class_one == '金融理财') & (s.perc <= thres))
		print('\tRecall for Top {:.0f}% is {:.4f}'.format(thres*100, n_recalled/n_class_one))