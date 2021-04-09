# -*- coding: utf-8 -*-

import argparse
import time

import numpy as np
import pandas as pd
import torch
import torch.nn.functional as F
import torch.optim as optim

from torch.utils.data import DataLoader

from app_dataset import APPDataset
from mlp_model import MLP

def accuracy(y_hat, y):
	preds = y_hat.max(1)[1].type_as(y)
	comps = preds.eq(y).double()
	comps = comps[y==1].sum()
	return comps / y.sum()#(len(y)-y.sum())

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	#parser.add_argument('--seed', type=int, default=10, help='Random seed.')
	parser.add_argument('--train_path', type=str, default='./app_data_train.csv', help='File path of training data')
	parser.add_argument('--eval_path', type=str, default='./app_data_eval.csv', help='File path of evaluation data')
	parser.add_argument('--test_path', type=str, default='./app_data_test.csv', help='File path of testing data')
	parser.add_argument('--epochs', type=int, default=120, help='Number of epochs to train.')
	parser.add_argument('--lr', type=float, default=3e-2, help='Initial learning rate of Adam.')
	parser.add_argument('--weight_decay', type=float, default=1e-4, help='Weight decay (L2 loss on parameters).')
	parser.add_argument('--verbose', type=int, default=1, help='Frequency to print evaluation performance.')
	parser.add_argument('--is_testing', dest='mode', action='store_true', default=False, help='Output the performance on the testing set.')
	parser.add_argument('--is_saving', action='store_true', default=False, help='Save the model locally.')
	parser.add_argument('--model_path', type=str, default='./app_mlp', help='File path of the saved model.')
	args = parser.parse_args()

	# Load data
	training_dataset = APPDataset(args.train_path)
	training_loader = DataLoader(dataset=training_dataset, batch_size=len(training_dataset), shuffle=True, num_workers=1)
	evaluation_dataset = APPDataset(args.eval_path)
	evaluation_loader = DataLoader(dataset=evaluation_dataset, batch_size=len(evaluation_dataset), shuffle=True, num_workers=1)
	testing_dataset = APPDataset(args.test_path)
	testing_loader = DataLoader(dataset=testing_dataset, batch_size=len(testing_dataset), shuffle=True, num_workers=1)

	# Initialize model
	np.random.seed(3)
	torch.manual_seed(10)
	layers = [16, 16]
	model = MLP(n_in=24, n_out=2, n_hids=layers)

	# Train model
	optimizer = optim.Adam(model.parameters(), lr=args.lr, weight_decay=args.weight_decay)
	start_time = time.time()
	epoch_time = start_time
	for i in range(args.epochs):
		print('====> Epoch {}'.format(i))
		# Step: training
		for x, y in training_loader:
			model.train()
			optimizer.zero_grad()
			y_hat = model(x)
			loss_train = F.nll_loss(y_hat, y, weight=torch.tensor([1.0, 69.0]))
			acc_train = accuracy(y_hat, y)
			loss_train.backward()
			optimizer.step()
			# Step: evaluation
			y_hat_eval = torch.zeros((len(evaluation_dataset), 2))
			y_eval = torch.zeros((len(evaluation_dataset), ), dtype=torch.long)
			fr = 0
			model.eval()
			with torch.no_grad():
				for x, y in evaluation_loader:
					batch_size = len(y)
					y_hat_eval[fr:(fr+batch_size), :] = model(x)
					y_eval[fr:(fr+batch_size)] = y
					fr += batch_size
			loss_eval = F.nll_loss(y_hat_eval, y_eval, weight=torch.tensor([1.0, 69.0]))
			acc_eval = accuracy(y_hat_eval, y_eval)
			curr_time = time.time()
			print('Epoch: {:04d}'.format(i+1),
					'loss_train: {:.4f}'.format(loss_train.item()),
					'acc_train: {:.4f}'.format(acc_train.item()),
					'loss_eval: {:.4f}'.format(loss_eval.item()),
					'acc_eval: {:.4f}'.format(acc_eval.item()),
					'duration: {:.4f}s'.format(curr_time-epoch_time))
			epoch_time = curr_time

	if args.mode:
		model.eval()
		with torch.no_grad():
			y_hat_test = None
			y_test = None
			for x, y in testing_loader:
				y_hat_test = model(x)
				y_test = y
			loss_test = F.nll_loss(y_hat_test, y_test, weight=torch.tensor([1.0, 69.0]))
			acc_test = accuracy(y_hat_test, y_test)
			print("Test set results:",
					"loss={:.4f}".format(loss_test.item()),
					"accuracy={:.4f}".format(acc_test.item()))

	if args.is_saving:
		torch.save(model.state_dict(), '{}_L{}.pth'.format(args.model_path, '-'.join([str(l) for l in layers])))