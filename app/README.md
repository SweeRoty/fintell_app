### Prepare vertices
1. `cd vertices/`
2. Prepare valid devices for vertex and edge computing: `spark-submit prepareValidDevices.py --fr 20200801 --to 20200807`
3. Extract raw app vertices from daily installed app lists (delivered to JiGuang): `sh extract_raw_vertices.sh 20200801 20200807`
4. Prepare final app vertices: `spark-submit prepareVertices.py --fr 20200801 --to 20200807`
5. Perform local analysis to decide how much percentage apps to be covered in the app graph, refer to http://112.74.230.70/pages/viewpage.action?pageId=14091470

### Prepare edges
1. `cd edges/`
2. Extract edges connecting valid vertices based on their total frequencies (delivered to JiGuang): `spark-submit extractEdges.py --fr 20200801 --to 20200801 --device_date 20200814`

### Detect communities locally using TigerGraph
1. Download vertex and edge files to the local environment
2. `cd community_detection`
3. Load graph data into TigerGraph if not imported: `gsql ` 
4. Calculate the number of connected components (optional): `gsql `
5. Detect communities using Louvain: ``
