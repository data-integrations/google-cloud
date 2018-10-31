# Google Cloud Spanner Sink

Description
-----------
This sink writes to a Google Cloud Spanner table.
Cloud Spanner is a fully managed, mission-critical, relational database service that offers transactional
consistency at global scale, schemas, SQL (ANSI 2011 with extensions),
and automatic, synchronous replication for high availability.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access Google Cloud Spanner.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Instance ID**: Instance the Spanner database belongs to. Spanner instance is contained within a specific project.
 Instance is an allocation of resources that is used by Cloud Spanner databases created in that instance.

**Database Name**: Database the Spanner table belongs to.
Spanner database is contained within a specific Spanner instance. If the database does not exist, it will get created.

**Table Name**: Table to write to. A table contains individual records organized in rows.
Each record is composed of columns (also called fields).
Every table is defined by a schema that describes the column names, data types, and other information.
If the table does not exist, it will get created.

**Primary Key**: If the table does not exist, a primary key must be provided in order to auto-create the table.
The key can be a composite key of multiple fields in the schema. This is not required if the table already exists.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Write Batch Size**: Size (in number of records) of the batched writes to the Spanner table.
Each write to Cloud Spanner contains some overhead. To maximize bulk write throughput,
maximize the amount of data stored per write. A good technique is for each commit to mutate hundreds of rows.
Commits with the number of mutations in the range of 1 MiB - 5 MiB rows usually provide the best performance.
Default value is 100 mutations.

**Schema**: Schema of the data to write. Must be compatible with the table schema.
