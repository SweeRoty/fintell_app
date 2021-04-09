## MLP
1. `cd neural_network/mlp`
2. Preprocess the features by `python preprocess.py --x_path $x_path --sample $sample`. Arguments *x_path* and *sample* specify the paths of raw feature file and the sample file
3. Start training the model `python train.py`. Please refer to arguments in the python script 

## GCN
1. `cd neural_network/graph_convolution_network`
2. Prepare the data by `python preprocess.py --n_path $n_path --sample $sample --e_path $e_path`. Arguments *n_path*, *sample* and *e_path* specify the paths of the raw node file, the sample file and the raw edge file
3. Start training the model `python train.py`. Please refer to arguments in the python script 
4. Inference mode