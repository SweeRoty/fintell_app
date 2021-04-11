## APP Grap

### Step1: Extract APP samples (vertices)
* If the hive tables for storing APP samples are not created, create them first. Substitute the table names in the bash scripts if necessary.
* Execute the *start.sh* script. Change the IO path in the Spark scripts if needed.
  ```
  cd samples/
  cd preparation/ & sh modify_schema_for_vertex_raw.sh & sh modify_schema_for_vertex.sh & cd ..
  cd vertices/ & sh start.sh
  cd ../../
  ```

### Step 2: Extract APP edges
* If the hive table for storing APP edges is not created, create it first. Substitute the table name in the bash script accordingly.
* Execute the *start.sh* script. Change the IO path in the Spark scripts if needed.
  ```
  cd edges/
  cd preparation/ & sh modify_schema_for_edge.sh & cd ..
  sh start.sh
  cd ../
  ```

### Step3: Construct APP features
* If the hive tables for storing APP features are not ready, create them first. Substitute the table names in the bash scripts accordingly.
* Execute the *start.sh* script. Change the IO path in the Spark scripts if needed.
```
  cd features/
  cd preparation/ & sh modify_schema_for_ins_features.sh & sh modify_schema_for_dev_features.sh & cd ..
  sh start.sh
  cd ../
```
