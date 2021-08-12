# Google Cloud Spanner Connection

Description
-----------
Use this connection to access data in Google Cloud Spanner.

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
property is required in the request body. It can be in the following form :

1. `/{instance}/{database}/{table}`
   This path indicates a table. A table is the only one that can be sampled. Browse on this path to return the specified table.

2. `/{instance}/{database}`
   This path indicates a database. A database cannot be sampled. Browse on this path to get all the tables under this database.

3. `/{instance}`
   This path indicates a instance. A instance cannot be sampled. Browse on this path to get all the databases under this instance.

4. `/`
   This path indicates the root. A root cannot be sampled. Browse on this path to get all the instances visible through this connection.
