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

**Use Connection** Whether to use a connection. If a connection is used, you do not need to provide the credentials.

**Connection** Name of the connection to use. Project and service account information will be provided by the connection.
You also can use the macro function ${conn(connection-name)}.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console. This is the project
that the BigQuery job will run in. `BigQuery Job User` role on this project must be granted to the specified service
account to run the job. If a temporary bucket needs to be created, the bucket will also be created in this project and
'GCE Storage Bucket Admin' role on this project must be granted to the specified service account to create buckets.

**Dataset Project ID**: Project the dataset belongs to. This is only required if the dataset is not
in the same project that the BigQuery job will run in. If no value is given, it will default to the 
configured Project ID. `BigQuery Data Editor` role on this project must be granted to the specified service account to
write BigQuery data to this project.

**Dataset**: Dataset the table belongs to. A dataset is contained within a specific project.
Datasets are top-level containers that are used to organize and control access to tables and views.

**Table**: Table to write to. A table contains individual records organized in rows.
Each record is composed of columns (also called fields).
Every table is defined by a schema that describes the column names, data types, and other information.

**Temporary Bucket Name**: Google Cloud Storage bucket to store temporary data in.
Temporary data will be deleted after it is loaded into BigQuery. If it is not provided, a unique
bucket will be created and then deleted after the run finishes.

**GCS Upload Request Chunk Size**: GCS upload request chunk size in bytes. Default value is 8388608 bytes.

**JSON String**: List of fields to be written to BigQuery as a JSON string.
The fields must be of type STRING. To target nested fields, use dot notation.
For example, 'name.first' will target the 'first' field in the 'name' record. (Macro Enabled)

Use a comma-separated list to specify multiple fields in macro format.
Example: "nestedObject.nestedArray.raw, nestedArray.raw".

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

**Encryption Key Name**: Used to encrypt data written to any bucket, dataset, or table created by the plugin.
If the bucket, dataset, or table already exists, this is ignored. More information can be found 
[here](https://cloud.google.com/data-fusion/docs/how-to/customer-managed-encryption-keys)

**Create Partitioned Table  [DEPRECATED]**: Whether to create the BigQuery table with time partitioning. This value
is ignored if the table already exists.
* When this is set to true, table will be created with time partitioning.
* When this is set to false, value of Partitioning type will be used.
* [DEPRECATED] use Partitioning Type

**Partitioning Type**: Specifies the partitioning type. Can either be Integer or Time or None. Defaults to Time.
  This value is ignored if the table already exists.
* When this is set to Time, table will be created with time partitioning.
* When this is set to Integer, table will be created with range partitioning.
* When this is set to None, table will be created without time partitioning.

**Time Partitioning Type**: Specifies the time partitioning type. Can either be Daily or Hourly or Monthly or Yearly.
Default is Daily. Ignored when table already exists

**Range Start**: For integer partitioning, specifies the start of the range. Only used when table doesn’t 
exist already, and partitioning type is set to Integer.
* The start value is inclusive.

**Range End**: For integer partitioning, specifies the end of the range. Only used when table doesn’t 
exist already, and partitioning type is set to Integer.
* The end value is exclusive.

**Range Interval**: For integer partitioning, specifies the partition interval. Only used when table doesn’t exist already,
and partitioning type is set to Integer.
* The interval value must be a positive integer.
 
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

**Service Account**  - service account key used for authorization

* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

* **JSON**: Contents of the service account JSON file.

**Schema**: Schema of the data to write. 
If a schema is provided, it must be compatible with the table schema in BigQuery.

Data Type Mappings from CDAP to BigQuery
----------
The following table lists out different CDAP data types, as well as the 
corresponding BigQuery data type for each CDAP type, for updates and upserts.

| CDAP type      | BigQuery type                      |
|----------------|------------------------------------|
| array          | repeated                           |
| boolean        | bool                               |
| bytes          | bytes                              |
| date           | date                               |
| datetime       | datetime, string                   |
| decimal        | numeric, bignumeric                |
| double / float | float64                            |
| enum           | unsupported                        |
| int / long     | int64                              |
| map            | unsupported                        |
| record         | struct                             |
| string         | string, datetime (ISO 8601 format) |
| time           | time                               |
| timestamp      | timestamp                          |
| union          | unsupported                        |

For inserts, the type conversions are the same as those used in loading Avro
data to BigQuery; the table is available
[here](https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-avro#avro_conversions).

Trouble Shooting
----------------
**missing permission to create a temporary bucket**
If your pipeline failed with the following error in the log:
```
com.google.api.client.googleapis.json.GoogleJsonResponseException: 403 Forbidden
POST https://storage.googleapis.com/storage/v1/b?project=projectId&projection=full
{
"code" : 403,
"errors" : [ {
"domain" : "global",
"message" : "xxxxxxxxxxxx-compute@developer.gserviceaccount.com does not have storage.buckets.create access to the Google Cloud project.",
"reason" : "forbidden"
} ],
"message" : "xxxxxxxxxxxx-compute@developer.gserviceaccount.com does not have storage.buckets.create access to the Google Cloud project."
}
```
`xxxxxxxxxxxx-compute@developer.gserviceaccount.com` is the service account you specified in this plugin. This
means the temporary bucket you specified in this plugin doesn't exist. CDF/CDAP is trying to create the temporary bucket,
but the specified service account doesn't have the permission. You must grant "GCE Storage Bucket Admin" role on the
project identified by the `Project ID` you specified in this plugin to the service account. If you think you already
granted the role, check if you granted the role to the wrong project (for example the one identified by the
`Dataset Project ID`).

**missing permission to run BigQuery jobs**

If your pipeline failed with the following error in the log:
```
POST https://bigquery.googleapis.com/bigquery/v2/projects/xxxx/jobs
{
"code" : 403,
"errors" : [ {
"domain" : "global",
"message" : "Access Denied: Project xxxx: User does not have bigquery.jobs.create permission in project xxxx",
"reason" : "accessDenied"
} ],
"message" : "Access Denied: Project xxxx: User does not have bigquery.jobs.create permission in project xxxx.",
"status" : "PERMISSION_DENIED"
}
``` 
`xxxx` is the `Project ID` you specified in this plugin. This means the specified service account doesn't have the
permission to run BigQuery jobs. You must grant "BigQuery Job User" role on the project identified by the `Project ID`
you specified in this plugin to the service account. If you think you already granted the role, check if you granted the
role on the wrong project (for example the one identified by the `Dataset Project ID`).

**missing permission to create the BigQuery dataset**

If your pipeline failed with the following error in the log:
```
POST https://www.googleapis.com/bigquery/v2/projects/xxxx/datasets?prettyPrint=false
{
  "code" : 403,
  "errors" : [ {
    "domain" : "global",
    "message" : "Access Denied: Project xxxx: User does not have bigquery.datasets.create permission in project xxxx.",
    "reason" : "accessDenied"
  } ],
  "message" : "Access Denied: Project xxxx: User does not have bigquery.datasets.create permission in project xxxx.",
  "status" : "PERMISSION_DENIED"
}
``` 
`xxxx` is the `Dataset Project ID` you specified in this plugin. This means the dataset specified in this plugin doesn't
exist. CDF/CDAP is trying to create the dataset but the service account you specified in this plugin doesn't have the permission.
You must grant "BigQuery Data Editor" role on the project identified by the `Dataset Project ID` you specified in this
plugin to the service account. If you think you already granted the role, check if you granted the role on the wrong
project (for example the one identified by the `Project ID`).

**missing permission to create the BigQuery table**
If your pipeline failed with the following error in the log:
```
POST https://bigquery.googleapis.com/bigquery/v2/projects/xxxx/jobs
{
"code" : 403,
"errors" : [ {
"domain" : "global",
"message" : "Access Denied: Dataset xxxx:mysql_bq_perm: Permission bigquery.tables.create denied on dataset xxxx:mysql_bq_perm (or it may not exist).",
"reason" : "accessDenied"
} ],
"message" : "Access Denied: Dataset xxxx:mysql_bq_perm: Permission bigquery.tables.create denied on dataset xxxx:mysql_bq_perm (or it may not exist).",
"status" : "PERMISSION_DENIED"
}
```
`xxxx` is the `Dataset Project ID` you specified in this plugin. This means the table specified in this plugin doesn't
exist. CDF/CDAP is trying to create the table but the service account you specified in this plugin doesn't have the permission.
You must grant "BigQuery Data Editor" role on the project identified by the `Dataset Project ID` you specified in this
plugin to the service account. If you think you already granted the role, check if you granted the role on the wrong
project (for example the one identified by the `Project ID`).

**missing permission to read the BigQuery dataset**
If your pipeline failed with the following error in the log:
```
com.google.api.client.googleapis.json.GoogleJsonResponseException: 403 Forbidden
GET https://www.googleapis.com/bigquery/v2/projects/xxxx/datasets/mysql_bq_perm?prettyPrint=false
{
"code" : 403,
"errors" : [ {
"domain" : "global",
"message" : "Access Denied: Dataset xxxx:mysql_bq_perm: Permission bigquery.datasets.get denied on dataset xxxx:mysql_bq_perm (or it may not exist).",
"reason" : "accessDenied"
} ],
"message" : "Access Denied: Dataset xxxx:mysql_bq_perm: Permission bigquery.datasets.get denied on dataset xxxx:mysql_bq_perm (or it may not exist).",
"status" : "PERMISSION_DENIED"
}
```
`xxxx` is the `Dataset Project ID` you specified in this plugin. The service account you specified in this plugin doesn't
have the permission to read the dataset you specified in this plugin. You must grant "BigQuery Data Editor" role on the
project identified by the `Dataset Project ID` you specified in this plugin to the service account. If you think you
already granted the role, check if you granted the role on the wrong project (for example the one identified by the `Project ID`).
