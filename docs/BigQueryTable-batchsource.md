# Google BigQuery Table Source

Description
-----------
This source reads the entire contents of a BigQuery table.
BigQuery is Google's serverless, highly scalable, enterprise data warehouse.
Data from the BigQuery table is first exported to a temporary location on Google Cloud Storage,
then read into the pipeline from there.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access BigQuery and Google Cloud Storage.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Use Connection** Whether to use a connection. If a connection is used, you do not need to provide the credentials.

**Connection** Name of the connection to use. Project and service account information will be provided by the connection.
You also can use the macro function ${conn(connection-name)}.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console. This is the project
that the BigQuery job will run in. `BigQuery Job User` role on this project must be granted to the specified service
account to run the job. If a temporary bucket needs to be created, the bucket will also be created in this project and
'GCE Storage Bucket Admin' role on this project must be granted to the specified service account to create buckets. 

**Dataset Project ID**: Project the dataset belongs to. This is only required if the dataset is not
in the same project that the BigQuery job will run in. If no value is given, it will default to the configured Project
ID. `BigQuery Data Viewer` role on this project must be granted to the specified service account to read BigQuery data
from this project.

**Dataset**: Dataset the table belongs to. A dataset is contained within a specific project.
Datasets are top-level containers that are used to organize and control access to tables and views.

**Table**: Table to read from. A table contains individual records organized in rows.
Each record is composed of columns (also called fields).
Every table is defined by a schema that describes the column names, data types, and other information.

**Partition Start Date**: Inclusive partition start date, specified as 'yyyy-MM-dd'. For example, '2019-01-01'. 
If no value is given, all partitions up to the partition end date will be read.

**Partition End Date**: Exclusive partition end date, specified as 'yyyy-MM-dd'. For example, '2019-01-01'. 
If no value is given, all partitions up from the partition start date will be read.

**Filter**: Filters out rows that do not match the given condition. For example, if the filter is 'age > 50 and 
name is not null', all output rows will have an 'age' over 50 and a value for the 'name' field.
This is the same as the WHERE clause in BigQuery. More information can be found at
https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#where_clause

**Enable Querying Views**: Whether to allow querying views. Since BigQuery views are not materialized 
by default, querying them may have a performance overhead.

**Temporary Table Creation Project**: The project name where the temporary table should be created. Defaults 
to the same project in which the table is located.

**Temporary Table Creation Dataset**: The dataset in the specified project where the temporary table should be
created. Defaults to the same dataset in which the table is located.

**Temporary Bucket Name**: Google Cloud Storage bucket to store temporary data in.
Temporary data will be deleted after it has been read. If it is not provided, a unique bucket will be
created and then deleted after the run finishes.

**Encryption Key Name**: Used to encrypt data written to any bucket created by the plugin.
If the bucket already exists, this is ignored. More information can be found 
[here](https://cloud.google.com/data-fusion/docs/how-to/customer-managed-encryption-keys)

**Service Account**  - service account key used for authorization

* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

* **JSON**: Contents of the service account JSON file.

**Schema**: Schema of the table to read. This can be fetched by clicking the 'Get Schema' button.

Data Type Mappings from BigQuery to CDAP
----------
The following table lists out different BigQuery data types, as well as the 
corresponding CDAP data type for each BigQuery type.

| BigQuery type | CDAP type                          |
|---------------|------------------------------------|
| bool          | boolean                            |
| bytes         | bytes                              |
| date          | date                               |
| datetime      | datetime, string                   |
| float64       | double                             |
| geo           | unsupported                        |
| int64         | long                               |
| numeric       | decimal                            |
| bignumeric    | decimal                            |
| repeated      | array                              |
| string        | string, datetime (ISO 8601 format) |
| struct        | record                             |
| time          | time (microseconds)                |
| timestamp     | timestamp (microseconds)           |
| json          | string                             |
