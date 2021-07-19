# Google Cloud Spanner Connector

Description
-----------
This plugin can be used to browse and sample data from Google Cloud Spanner.

Cloud Spanner is the only enterprise-grade, globally-distributed, and strongly-consistent database service built for the
cloud, specifically to combine the benefits of relational database structure with non-relational horizontal scale. It is
a unique database that combines transactions, SQL queries, and relational structure with the scalability that you
typically associate with non-relational or NoSQL databases.

Properties
----------
**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Service Account**  - When running on Google Cloud Platform, the service account key does not need to be provided, 
as it can automatically be read from the environment. In other environments, the service account key must be provided.

* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

* **JSON**: Contents of the service account JSON file.