# Google Cloud Spanner Connector

Description
-----------
Use this connection to access data in Google Cloud Spanner.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Service Account**  - When running on Google Cloud Platform, the service account key does not need to be provided, 
as it can automatically be read from the environment. In other environments, the service account key must be provided.

* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

* **JSON**: Contents of the service account JSON file.