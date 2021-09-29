# Google Dataplex Connection

Description
-----------
Use this connection to access data in Google Dataplex.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project. This is where the job will be running
to query the data from Dataplex. It can be found on the Dashboard in the Google Cloud Platform Console. Make sure the service account
specified below has the permission to access Dataplex in this project.

**Service Account**: When running on Google Cloud Platform, the service account key does not need to be provided, as
it can automatically be read from the environment. In other environments, the service account key must be provided.

* **File Path**: Path on the local file system of the service account key used for authorization. Can be set to '
  auto-detect' when running on a Dataproc cluster. When running on other clusters, the file must be present on every
  node in the cluster.

* **JSON**: Contents of the service account JSON file.


Path of the connection
----------------------
To browse, get a sample from, or get the specification for this connection through
[Pipeline Microservices](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/975929350/Pipeline+Microservices), the `path`
property is required in the request body. It can be in the following form :

1. `/{location}/{lake}/{zone}/{asset}`
   This path indicates a asset. A asset is the only one that can be sampled. Browse on this path to return the specified asset.

2. `/{location}/{lake}/{zone}/`
   This path indicates a zone. A zone cannot be sampled. Browse on this path to get all the assets under this zone.

3. `/{location}/{lake}/`
   This path indicates a lake. A lake cannot be sampled. Browse on this path to get all the zones under this lake.

4. `/{location}`
   This path indicates a location. A location cannot be sampled. Browse on this path to get all the lakes under this location.

5. `/`
   This path indicates the root. A root cannot be sampled. Browse on this path to get all the locations visible through this connection.
