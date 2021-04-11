# -*- coding: utf-8 -*-

import torch.nn as nn
import torch.nn.functional as F

class MLP(nn.Module):

	def __init__(self, n_in, n_out, n_hids):
		super(MLP, self).__init__()

		self.layers = []
		n_hids.insert(0, n_in)
		n_hids.append(n_out)
		for i in range(len(n_hids)-1):
			self.layers.append(nn.Linear(n_hids[i], n_hids[i+1]))
		self.layers = nn.ModuleList(self.layers)

	def forward(self, x):
		for i, layer in enumerate(self.layers):
			x = F.relu(layer(x))
			x = F.dropout(x, 0.25)
		return F.log_softmax(x, dim=1)

	def __repr__(self):
		return self.__class__.__name__ + ' [' \
     			+ ' connects with '.join([str(layer) for layer in self.layers]) + ']'