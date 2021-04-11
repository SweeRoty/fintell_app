# -*- coding: utf-8 -*-

import argparse
import time

from sklearn.metrics import roc_auc_score
import numpy as np
import pandas as pd
import torch
import torch.nn.functional as F

from gcn_data import GCNData
from gcn_model import GCN

def load_app_graph(node_path, edge_path):
	"""Load the APP graph using pandas IO
		node format: the 1st column should be ID; the last 2 colums should be flag indicating which set the sample belong to, and label
		edge format:
	"""
	x = pd.read_csv(node_path)
	y = x.iloc[:, -2].values.reshape((-1, ))
	phase = x.iloc[:, -1].values.reshape((-1, ))
	x = x.iloc[:, 1:-2].values

	edges = pd.read_csv(edge_path).values

	data_loader = GCNData(x, y, edges)

	idx_train = torch.LongTensor(np.where(phase == 0)[0])
	idx_eval = torch.LongTensor(np.where(phase == 1)[0])
	idx_test = torch.LongTensor(np.where(phase == 2)[0])

	return data_loader.x, data_loader.y, data_loader.adj, idx_train, idx_eval, idx_test

def get_activation(name):
	global activation
	def hook(model, input, output):
		activation[name] = output.detach()
	return hook

def accuracy(y_hat, y):
	preds = y_hat.max(1)[1].type_as(y)
	comps = preds.eq(y).double()
	comps = comps.sum()
	return comps / len(y)

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument('--node_path', type=str, default='../../data/app_nodes.csv', help='X')
	parser.add_argument('--edge_path', type=str, default='../../data/app_edges.csv', help='Adj')
	parser.add_argument('--model_path', type=str, default='./app_gcn_L2-8-8-2.pth', help='GCN')
	parser.add_argument('--is_intermediate', action='store_true', default=False)
	parser.add_argument('--is_acc', action='store_true', default=False)
	parser.add_argument('--is_auc', action='store_true', default=False)
	args = parser.parse_args()

	# Load data
	x, y, adj, idx_train, idx_eval, idx_test = load_app_graph(args.node_path, args.edge_path)

	# Load model
	model = GCN(n_in=x.shape[1], n_out=y.max().item()+1, n_hids=[8, 8])
	model.load_state_dict(torch.load(args.model_path))
	model.eval()

	activation = {}
	start_time = time.time()
	with torch.no_grad():
		if args.is_intermediate:
			model.layers[1].register_forward_hook(get_activation('intermediate'))
		y_hat = model(x, adj)
		if args.is_acc:
			loss = F.nll_loss(y_hat[idx_test], y[idx_test])#, weight=torch.tensor([1.0, 69.0]))
			acc = accuracy(y_hat[idx_test], y[idx_test])
			print('NLL_loss is: {:.4f}'.format(loss.item()), 'ACC is: {:.4f}'.format(acc.item()))
	end_time = time.time()
	print('Duration of inference is: {:.4f}s'.format(end_time - start_time))

	y_hat = y_hat.numpy()
	if args.is_auc:
		idx_test = idx_test.numpy()
		pred_test = y_hat[idx_test]
		pred_test = pred_test[:, 1]
		y = y.numpy()
		true_test = y[idx_test]
		print('AUC on the testing set is {:.6f}'.format(roc_auc_score(true_test, pred_test)))
	np.save('../../data/app_gcn_layer8-8_v2_prd', y_hat)
	if args.is_intermediate:
		encodings = activation['intermediate'].numpy()
		np.save('../../data/app_gcn_layer8-8_v2_enc', encodings)