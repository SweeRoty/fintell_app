# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

if __name__ == '__main__':
	samples = pd.read_csv('./samples_0901_2799.csv', usecols=['app_package', 'app_index', 'class_one', 'flag_debt', 'flag_714'])
	samples['label'] = samples[['flag_debt', 'flag_714']].any(axis=1)
	samples.loc[samples.class_one.isin(['购物优惠', '金融理财']) & (samples.label != 1), 'label'] = -1
	samples.loc[samples.class_one.isna() & (samples.label != 1), 'label'] = -2

	np.random.seed(186)
	samples['seed'] = np.random.rand(samples.shape[0])
	samples['group_index'] = samples.groupby('label')['seed'].rank('dense')
	samples['phase'] = -1
	samples.loc[samples.label >= 0, 'phase'] = 0
	pos_sum = np.sum(samples.label.values == 1)
	samples.loc[(samples.label == 1) & (samples.group_index > pos_sum*0.8), 'phase'] = 2
	samples.loc[(samples.label == 1) & (samples.group_index > pos_sum*0.6) & (samples.phase != 2), 'phase'] = 1
	neg_sum = np.sum(samples.label.values == 0)
	samples.loc[(samples.label == 0) & (samples.group_index > neg_sum*0.8), 'phase'] = 2
	samples.loc[(samples.label == 0) & (samples.group_index > neg_sum*0.6) & (samples.phase == 0), 'phase'] = 1

	samples.sort_values('app_index', inplace=True)
	samples['app_index'] = np.arange(1, samples.shape[0]+1)
	samples.loc[samples.label < 0, 'label'] = False
	samples[['app_package', 'app_index', 'label', 'phase']].to_csv('./final_samples_0901_2799.csv', index=False)