## fintell_app

### Extract APP samples
1. Enter the sub directory `cd samples/`
2. If the hive tables for storing APP samples are not ready, create them by `cd preparation/` and `sh modify_schema_for_vertex_raw.sh` `sh modify_schema_for_vertex.sh` `cd ..`. Substitute the table names in the bash scripts accordingly.
3. Prepare the vertices by `cd vertices/` and `sh start.sh`. Change the IO path in the Spark scripts if needed.
4. `cd ../../`

### Extract APP edges
1. Enter the sub directory `cd edges`
2. If the hive table for storing APP edges is not ready, create it by `cd preparation/` and `sh modify_schema_for_edge.sh` `cd ..`. Substitute the table name in the bash script accordingly.
3. Prepare the edges by `sh start.sh`
4. `cd ../`

### Construct APP features
1. Enter the sub directory `cd features/`
2. If the hive tables for storing APP features are not ready, create them by `cd preparation/` and `sh modify_schema_for_dev_features.sh` `sh modify_schema_for_ins_features.sh` `cd ..`. Substitute the table names in the bash scripts accordingly.
