## MLP
1. `cd neural_network/mlp`
2. Preprocess the features `python feature_engineering.py --sample $sample --x_path $x_path`. *sample* and *x_path* specify the paths of the sample file and raw feature file
3. Start train the model `python train.py` Please refer to parameters in the python script 

## GCM
1. `cd neural_network/graph_convolution_network`
2. Extract edges connecting valid vertices based on their total frequencies (delivered to JiGuang): `spark-submit extractEdges.py --fr 20200801 --to 20200801 --device_date 20200814`

## Detect communities locally on TigerGraph
1. Download the vertex (nodes.csv) and edge (edges.csv) files into the local TigerGraph environment
2. `cd community_detection`
3. Adapt the gsql templates for this task (specify the Vertex name, Edge name, Graph name, input and output locations): `sh adapt.sh .\\/nodes.csv .\\/edges.csv APP SharedDevice APP_Graph .\\/output\\/communities.csv`
4. Load the graph data into TigerGraph: `gsql load_graph.gsql` 
5. Calculate the number of connected components (optional): `gsql conn_comp.gsql`
6. Detect communities using Louvain: `gsql louvain.gsql`
