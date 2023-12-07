# Google BigQuery Multi Table Sink

Description
-----------
This sink writes to a multiple BigQuery tables.
BigQuery is Google's serverless, highly scalable, enterprise data warehouse.
Data is first written to a temporary location on Google Cloud Storage, then loaded into BigQuery from there.

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

**Project ID:** Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console. This is the project
that the BigQuery job will run in. `BigQuery Job User` role on this project must be granted to the specified service
account to run the job. If a temporary bucket needs to be created, the bucket will also be created in this project and
'GCE Storage Bucket Admin' role on this project must be granted to the specified service account to create buckets.

**Dataset Project ID**: Project the dataset belongs to. This is only required if the dataset is not
in the same project that the BigQuery job will run in. If no value is given, it will default to the 
configured Project ID. `BigQuery Data Editor` role on this project must be granted to the specified service account to
write BigQuery data to this project.

**Dataset:** Dataset the tables belongs to. A dataset is contained within a specific project.
Datasets are top-level containers that are used to organize and control access to tables and views.
If dataset does not exist, it will be created.

**BQ Job Labels:** Key value pairs to be added as labels to the BigQuery job. Keys must be unique. (Macro Enabled)

[job_source, type] are system defined labels used by CDAP for internal purpose and cannot be used as label keys.
Macro format is supported. example `key1:val1,key2:val2`

Keys and values can contain only lowercase letters, numeric characters, underscores, and dashes.
For more information about labels, see [Docs](https://cloud.google.com/bigquery/docs/labels-intro#requirements).

**Temporary Bucket Name:** Google Cloud Storage bucket to store temporary data in.
It will be automatically created if it does not exist. Temporary data will be deleted after it is loaded into BigQuery.
If the bucket was created automatically, it will be deleted after the run finishes.

**GCS Upload Request Chunk Size**: GCS upload request chunk size in bytes. Default value is 8388608 bytes.

**Truncate Table:** Whether or not to truncate the table before writing to it.
Should only be used with the Insert operation.

**Location:** The location where the big query datasets will get created. This value is ignored
if the dataset or temporary bucket already exist.

**Encryption Key Name**: Used to encrypt data written to any bucket, dataset, or table created by the plugin.
If the bucket, dataset, or table already exists, this is ignored. More information can be found 
[here](https://cloud.google.com/data-fusion/docs/how-to/customer-managed-encryption-keys)

**Split Field:** The name of the field that will be used to determine which table to write to. If unspecified, it
defaults to `tablename`. This field is used when Allow Flexible Schemas in Output configuration is enabled.

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

**Allow Flexible Schemas in Output**: When enabled, this sink will write out records with arbitrary schemas.
Records may not have a well defined schema depending on the source.
When disabled, table schemas must be passed in pipeline arguments.

**Service Account**  - service account key used for authorization

* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

* **JSON**: Contents of the service account JSON file.

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

Example
-------

Suppose the input records are:

| id  | name     | email            | tablename |
| --- | -------- | ---------------- | --------- |
| 0   | Samuel   | sjax@example.net | accounts  |
| 1   | Alice    | a@example.net    | accounts  |

| userid | item     | action | tablename |
| ------ | -------- | ------ | --------- |
| 0      | shirt123 | view   | activity  |
| 0      | carxyz   | view   | activity  |
| 0      | shirt123 | buy    | activity  |
| 0      | coffee   | view   | activity  |
| 1      | cola     | buy    | activity  |

The plugin will expect two pipeline arguments to tell it to write the first two records to an `accounts` table
and others records to an `activity` table.

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

Column Names
------------
A column name can contain the letters (a-z, A-Z), numbers (0-9), or underscores (_), and it must start with a letter or
underscore. For more flexible column name support, see
[flexible column names](https://cloud.google.com/bigquery/docs/schemas#flexible-column-names).