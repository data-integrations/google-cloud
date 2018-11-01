# Google Cloud Spanner Source

Description
-----------
This source reads from a Google Cloud Spanner table.
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
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Instance ID**: Instance the Spanner database belongs to. Spanner instance is contained within a specific project.
 Instance is an allocation of resources that is used by Cloud Spanner databases created in that instance.

**Database Name**: Database the Spanner table belongs to.
Spanner database is contained within a specific Spanner instance.

**Table Name**: Table to read from. A table contains individual records organized in rows.
Each record is composed of columns (also called fields).
Every table is defined by a schema that describes the column names, data types, and other information.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Max Partitions**: Maximum number of partitions. This may be set to the number of map tasks desired.
The maximum value is currently 200,000. This is only a hint.
The actual number of partitions returned may be smaller or larger than this maximum count request.

**Partition Size (Megabytes)**: Partition size in megabytes. The desired data size for each partition generated.
This is only a hint. The actual size of each partition may be smaller or larger than this size request.
More information about partition options can be found at https://cloud.google.com/spanner/docs/reference/rest/v1/PartitionOptions

**Schema**: Schema of the Spanner table to read.
