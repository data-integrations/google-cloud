# Google Dataplex Sink

Description
-----------
This sink writes to a Dataplex asset. Dataplex is an intelligent data fabric that unifies your distributed data to help
automate data management and power analytics at scale. In case of asset of type Storage Bucket, Files can be written in
various formats such as avro, parquet, and orc. In case of asset of type BigQuery Dataset, data is first written to a
temporary location on Google Cloud Storage, then loaded into BigQuery from there.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be provided and can be
set to 'auto-detect'. Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided. The service account
key can be found on the Dashboard in the Cloud Platform Console. Make sure the account key has permission to access
BigQuery, Google Cloud Storage and Dataplex. The service account key file needs to be available on every node in your
cluster and must be readable by all users running the job.

Permissions
-----------

Assign the following roles to the 
[Dataproc service account](https://cloud.google.com/data-fusion/docs/concepts/service-accounts) to grant access to
Dataplex:
- Dataplex Developer
- Dataplex Data Reader
- Metastore Metadata User
- Cloud Dataplex Service Agent
- Dataplex Metadata Reader

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project. It can be found on the Dashboard in the
Google Cloud Platform Console. This is the project that the Dataplex job will run in. If a temporary bucket needs to be
created, the service account must have permission in this project to create buckets.

**Service Account**  - service account key used for authorization

* **File Path**: Path on the local file system of the service account key used for authorization. Can be set to '
  auto-detect' when running on a Dataproc cluster. When running on other clusters, the file must be present on every
  node in the cluster.

* **JSON**: Contents of the service account JSON file.

**Location ID**: ID of the location in which the Dataplex lake has been created, which can be found on the details page
of the lake.

**Lake ID**: ID of the Dataplex lake, which can be found on the details page of the lake.

**Zone ID**: ID of the Dataplex zone, which can be found on the details page of the zone.

**Asset ID**: ID of the Dataplex Asset. It represents a cloud resource that is being managed within a lake as a member
of a zone.

**Asset Type**: Type of asset selected to ingest the data in Dataplex.

* BigQuery Dataset. Asset is of type BigQuery dataset.
* Storage Bucket. Asset is of type Storage Bucket.

BigQuery Dataset specific fields
----------

**Table Name**: Table to write to. A table contains individual records organized in rows. Each record is composed of
columns (also called fields). Every table is defined by a schema that describes the column names, data types, and other
information.

**Operation**: Type of write operation to perform. This can be set to Insert, Update, or Upsert.

* Insert. All records will be inserted in destination table.
* Update. Records that match on Table Key will be updated in the table. Records that do not match will be dropped.
* Upsert. Records that match on Table Key will be updated. Records that do not match will be inserted.

**Table Key**: List of fields that determines relation between tables during Update and Upsert operations.

**Dedupe By**: Column names and sort order used to choose which input record to update/upsert when there are multiple
input records with the same key. For example, if this is set to 'updated_time desc', then if there are multiple input
records with the same key, the one with the largest value for 'updated_time' will be applied.

**Partition Filter**: Partition filter that can be used for partition elimination during Update or Upsert operations.
Only use with Update or Upsert operations for tables where
**Require Partition Filter** is enabled. For example, if the table is partitioned and the Partition Filter is
'_PARTITIONTIME > "2020-01-01" and _PARTITIONTIME < "2020-03-01"', the update operation will be performed only in the
partitions meeting the criteria.

**Truncate Table**: Whether or not to truncate the table before writing to it. Only use with the Insert
operation.

**Update Table Schema**: Whether the BigQuery table schema should be modified when it does not match the schema expected
by the pipeline.

* When set to false, any mismatches between the schema expected by the pipeline and the schema in BigQuery will
  result in pipeline failure.
* When set to true, the schema in BigQuery will be updated to match the schema expected by the pipeline,
  assuming the schemas are compatible.

**Partitioning Type**: Specifies the partitioning type. Can either be Integer, Time, or None. Defaults to Time. This
value is ignored if the table already exists.

* When set to Time, the table will be created with time partitioning.
* When set to Integer, the table will be created with range partitioning.
* When set to None, the table will be created without time partitioning.

**Range Start**: For integer partitioning, specifies the start of the range. Only used when table doesn’t exist already,
and **Partitioning Type** is set to Integer.

* The start value is inclusive.

**Range End**: For integer partitioning, specifies the end of the range. Only used when table doesn’t exist already,
and **Partitioning Type** is set to Integer.

* The end value is exclusive.

**Range Interval**: For integer partitioning, specifies the partition interval. Only used when table doesn’t exist
already, and **Partitioning Type** is set to Integer.

* The interval value must be a positive integer.

**Partition Field**: Partitioning column for the BigQuery table. Leave blank if the BigQuery table is an
ingestion-time partitioned table.

**Require Partition Filter**: Whether to create a table that requires a partition filter. This value is ignored if the
table already exists.

* When set to true, the table will be created with required partition filter.
* When set to false, the table will be created without required partition filter.

**Clustering Order**: List of fields that determines the sort order of the data. Fields must be of type INT, LONG,
STRING, DATE, TIMESTAMP, BOOLEAN, or DECIMAL. Tables cannot be clustered on more than 4 fields. This value is only used
when the BigQuery table is automatically created and ignored if the table already exists.

Compatible changes fall under the following categories:

* the pipeline schema contains nullable fields that do not exist in the BigQuery schema. In this case, the new fields
  will be added to the BigQuery schema.
* the pipeline schema contains nullable fields that are non-nullable in the BigQuery schema. In this case, the fields
  will be modified to become nullable in the BigQuery schema.
* the pipeline schema does not contain fields that exist in the BigQuery schema. In this case, those fields in the
  BigQuery schema will be modified to become nullable.

Incompatible schema changes will result in pipeline failure.

**Schema**: Schema of the data to write. If a schema is provided, it must be compatible with the table schema in
BigQuery.

Data Type Mappings from CDAP to BigQuery Asset
----------
The following table lists out different CDAP data types, as well as the corresponding BigQuery data type for each CDAP
type, for updates and upserts.

| CDAP type      | BigQuery type |
|----------------|---------------|
| array          | repeated      |
| boolean        | bool          |
| bytes          | bytes         |
| date           | date          |
| datetime       | datetime, string|
| decimal        | numeric       |
| double / float | float64       |
| enum           | unsupported   |
| int / long     | int64         |
| map            | unsupported   |
| record         | struct        |
| string         | string, datetime(Should be ISO 8601 format)|
| time           | time          |
| timestamp      | timestamp     |
| union          | unsupported   |

For inserts, the type conversions are the same as those used in loading Avro data to BigQuery; the table is available
[here](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#avro_conversions).

Storage Bucket specific fields
----------

**Table Name**: Table to write to. In case of Asset of type Storage Bucket, a table is a directory where data would be
stored and read by dataplex discover jobs.

**Path Suffix:** Time format for the output directory that will be appended to the path. For example, the format '
yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'. If not specified, directory will be created
with default format 'yyyy-MM-dd-HH-mm'.

**Format:** Format to write the records in. The format for a raw zone must be one of ‘json’, ‘avro’, ‘parquet’, ‘csv’,
or ‘orc’. The format for a curated zone must be one of ‘avro’, ‘orc’, or ‘parquet’. If the format is a macro, only the
pre-packaged formats can be used.

**Update Dataplex Metadata:** Whether to update Dataplex metadata for the newly created entities 
If enabled, the pipeline will automatically copy the output schema to the destination Dataplex entities, 
and the automated Dataplex Discovery won't run for them.

**Schema:** Schema of the data to write. The 'avro' and 'parquet' formats require a schema but other formats do not.
