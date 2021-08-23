# Dataplex Connection

Description
-----------
Use this connection to access data in Dataplex.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Service Account**: When running on Google Cloud Platform, the service account key does not need to be provided, 
as it can automatically be read from the environment. In other environments, the service account key must be provided.

* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

* **JSON**: Contents of the service account JSON file.

Path of the connection
----------------------
To browse, get a sample from, or get the specification for this connection through
[Pipeline Microservices](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/975929350/Pipeline+Microservices), the `path`
property is required in the request body.

1. `/{locationId}/{lakeId}/{zoneId}/{assetId}`
   This path indicates an asset details related to a selected zone and its corresponding lake and location. Browse on this path to return the specified asset.

2. `/{locationId}/{lakeId}/{zoneId}`
   This path indicates a zone details related to a lake and its corresponding location. Browse on this path to get zone details.

3. `/{locationId}/{lakeId}`
   This path indicates a lake details and its corresponding location. Browse on this path to get lake details.

4. `/{locationId}`
   This path indicates dataplex location details. Browse on this path to get location details.

3. `/`
   This path indicates the root. A root cannot be sampled. Browse on this path to get all the locations visible through this connection.
