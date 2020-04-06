# Google BigQuery Table Sink

Description
-----------
This sink writes to a BigQuery table.
BigQuery is Google's serverless, highly scalable, enterprise data warehouse.
Data is first written to a temporary location on
Google Cloud Storage, then loaded into BigQuery from there.

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
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console. This is the project
that the BigQuery job will run in. If a temporary bucket needs to be created, the service account
must have permission in this project to create buckets.

**Dataset**: Dataset the table belongs to. A dataset is contained within a specific project.
Datasets are top-level containers that are used to organize and control access to tables and views.

**Table**: Table to write to. A table contains individual records organized in rows.
Each record is composed of columns (also called fields).
Every table is defined by a schema that describes the column names, data types, and other information.

**Temporary Bucket Name**: Google Cloud Storage bucket to store temporary data in.
It will be automatically created if it does not exist, but will not be automatically deleted.
Temporary data will be deleted after it is loaded into BigQuery. If it is not provided, a unique
bucket will be created and then deleted after the run finishes.

**Operation**: Type of write operation to perform. This can be set to Insert, Update or Upsert.
* Insert - all records will be inserted in destination table.
* Update - records that match on Table Key will be updated in the table. Records that do not match 
will be dropped.
* Upsert - records that match on Table Key will be updated. Records that do not match will be inserted.

**Truncate Table**: Whether or not to truncate the table before writing to it.
Should only be used with the Insert operation.

**Table Key**: List of fields that determines relation between tables during Update and Upsert operations.

**Dedupe By**: Column names and sort order used to choose which input record to update/upsert when there are
multiple input records with the same key. For example, if this is set to 'updated_time desc', then if there are
multiple input records with the same key, the one with the largest value for 'updated_time' will be applied.

**Partition Filter**: Partition filter that can be used for partition elimination during Update or 
Upsert operations. Should only be used with Update or Upsert operations for tables where 
require partition filter is enabled. For example, if the table is partitioned the Partition Filter 
'_PARTITIONTIME > "2020-01-01" and _PARTITIONTIME < "2020-03-01"', 
the update operation will be performed only in the partitions meeting the criteria.

**Location:** The location where the big query dataset will get created. This value is ignored
if the dataset or temporary bucket already exist.

**Create Partitioned Table**: Whether to create the BigQuery table with time partitioning. This value
is ignored if the table already exists.
* When this is set to true, table will be created with time partitioning.
* When this is set to false, table will be created without time partitioning.

**Partition Field**: Partitioning column for the BigQuery table. This should be left empty if the
BigQuery table is an ingestion-time partitioned table.

**Require Partition Filter**: Whether to create a table that requires a partition filter. This value
is ignored if the table already exists.
* When this is set to true, table will be created with required partition filter.
* When this is set to false, table will be created without required partition filter.

**Clustering Order**: List of fields that determines the sort order of the data. Fields must be of type
INT, LONG, STRING, DATE, TIMESTAMP, BOOLEAN or DECIMAL. Tables cannot be clustered on more than 4 fields.
 This value is only used when the BigQuery table is automatically created and ignored if the table 
 already exists.

**Update Table Schema**: Whether the BigQuery table schema should be modified 
when it does not match the schema expected by the pipeline.
* When this is set to false, any mismatches between the schema expected by the pipeline 
and the schema in BigQuery will result in pipeline failure.
* When this is set to true, the schema in BigQuery will be updated to match the schema 
expected by the pipeline, assuming the schemas are compatible.

Compatible changes fall under the following categories:
* the pipeline schema contains nullable fields that do not exist in the BigQuery schema. 
In this case, the new fields will be added to the BigQuery schema.
* the pipeline schema contains nullable fields that are non-nullable in the BigQuery schema. 
In this case, the fields will be modified to become nullable in the BigQuery schema.
* the pipeline schema does not contain fields that exist in the BigQuery schema.
In this case, those fields in the BigQuery schema will be modified to become nullable.
                         
Incompatible schema changes will result in pipeline failure.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Schema**: Schema of the data to write. 
If a schema is provided, it must be compatible with the table schema in BigQuery.

Data Type Mappings from CDAP to BigQuery
----------
The following table lists out different CDAP data types, as well as the 
corresponding BigQuery data type for each CDAP type, for updates and upserts.

| CDAP type      | BigQuery type |
|----------------|---------------|
| array          | repeated      |
| boolean        | bool          |
| bytes          | bytes         |
| date           | date          |
| decimal        | numeric       |
| double / float | float64       |
| enum           | unsupported   |
| int / long     | int64         |
| map            | unsupported   |
| record         | struct        |
| string         | string        |
| time           | time          |
| timestamp      | timestamp     |
| union          | unsupported   |

For inserts, the type conversions are the same as those used in loading Avro
data to BigQuery; the table is available
[here](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#avro_conversions).
