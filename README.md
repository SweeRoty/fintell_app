## fintell_app

### Extract APP samples
1. Enter the sub directory `cd samples/`
2. If the hive table for storing APP samples are not ready, just `cd preparation/` and `sh modify_schema_for_vertex_raw.sh` `sh modify_schema_for_vertex.sh` `cd ..`. 
Substitute the table name in the bash script accordingly.
3. `cd vertices/` and `sh start.sh`
