## fintell_app

### Extract APP samples
1. Enter the sub directory `cd samples/`
2. If the hive table for storing APP samples are not ready, create them by `cd preparation/` and `sh modify_schema_for_vertex_raw.sh` `sh modify_schema_for_vertex.sh` `cd ..`. 
Substitute the table name in the bash script accordingly.
3. Prepare the vertices by `cd vertices/` and `sh start.sh`. Change the IO path if needed.
