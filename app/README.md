### Prepare vertices
1. `cd vertices/`
2. Prepare valid devices for vertex and edge computing: `spark-submit prepareValidDevices.py --fr 20200801 --to 20200807`
3. Extract raw app vertices from daily installed app lists: `sh extract_raw_vertices.sh 20200801 20200807`
4. Prepare final app vertices: `spark-submit prepareVertices.py --fr 20200801 --to 20200807`

### Prepare edges
