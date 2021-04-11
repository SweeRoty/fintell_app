# -*- coding: utf-8 -*-

from torch.utils.data import Dataset

import numpy as np
import pandas as pd
import torch

class APPDataset(Dataset):

	def __init__(self, file_path):
		csv_data = pd.read_csv(file_path)
		self.x = csv_data.drop(['app_package', 'label'], axis=1).values.astype('float32')
		self.y = torch.LongTensor(np.where(self.one_hot_encode(csv_data.label.values))[1])

	def one_hot_encode(self, labels):
		"""One-hot encode categorical labels
		"""
		classes = set(labels)
		class_dict = {c:i for i, c in enumerate(classes)}
		label_indices = np.array(list(map(class_dict.get, labels)), dtype=np.int32)
		encoded_labels = np.zeros((len(label_indices), label_indices.max()+1))
		encoded_labels[np.arange(len(label_indices)), label_indices] = 1
		return encoded_labels

	def __len__(self):
		return self.x.shape[0]

	def __getitem__(self, idx):
		x = self.x[idx]
		y = self.y[idx]
		return x, y