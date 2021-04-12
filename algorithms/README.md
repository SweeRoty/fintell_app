## Modeling

### 1. MLP & LR
1. Preprocess the features first with scaling and some feature engineering
2. Then train the model

```
  cd mlp/
  python preprocess.py --x_path $x_path --sample $sample # x_path is the path of the raw features and sample is the path of the sample file
  python train.py
```

### 2. GCN
1. Prepare the data for GCN
2. Then train the model

```
  cd graph_convolution_network/
  python preprocess.py --n_path $n_path --sample $sample --e_path $e_path # n_path, sample and e_path specify the paths of the raw node file, the sample file and the raw edge file
  python train.py
```

*Find more about arguments in the train.py*
