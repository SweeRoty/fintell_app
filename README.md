## APP Graph

### Step1: Extract samples (vertices)
* If the hive tables for storing vertices are not created, create them first. Substitute the table names in the bash scripts if necessary.
* Execute the *start.sh* script. Change the IO path in the Spark scripts if necessary.

  ```
  cd samples/
  cd preparation/ & sh modify_schema_for_vertex_raw.sh & sh modify_schema_for_vertex.sh & cd ..
  cd vertices/ & sh start.sh
  cd ../../
  ```

### Step 2: Extract connections (edges)
* If the hive table for storing edges is not created, create it first. Substitute the table name in the bash script if necessary.
* Execute the *start.sh* script. Change the IO path in the Spark scripts if necessary.

  ```
  cd edges/
  cd preparation/ & sh modify_schema_for_edge.sh & cd ..
  sh start.sh
  cd ../
  ```

### Step3: Construct features
* If the hive tables for storing features are not created, create them first. Substitute the table names in the bash scripts accordingly.
* Execute the *start.sh* script. Change the IO path in the Spark scripts if necessary.

```
  cd features/
  cd preparation/ & sh modify_schema_for_ins_features.sh & sh modify_schema_for_dev_features.sh & cd ..
  sh start.sh
  cd ../
```

### Step 4: Modeling
* Please refer to the README file in the *algorithms* sub directory
