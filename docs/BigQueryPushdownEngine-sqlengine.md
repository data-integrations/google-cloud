# Google BigQuery Pushdown Engine

Description
-----------
The BigQuery pushdown engine offloads certains pipeline operations into BigQuery for execution.

Currently, the only supported operation is dataset join operations using the **Joiner** plugin.

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
**Use Connection:** Whether to use a connection, if a connection is used,
the credentials does not need to be provided.

**Connection:** Name of the connection to use, should use the macro function ${conn(connection-name)} to provide.
Project and service account information will be provided by the connection.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console. This is the project
that the BigQuery job will run in. `BigQuery Job User` role on this project must be granted to the specified service
account to run the job. If a temporary bucket needs to be created, the bucket will also be created in this project and
'GCE Storage Bucket Admin' role on this project must be granted to the specified service account to create buckets.

**Dataset Project ID**: Project the dataset belongs to. This is only required if the dataset is not
in the same project that the BigQuery job will run in. If no value is given, it will default to the 
configured Project ID. `BigQuery Data Owner` role on this project must be granted to the specified service account to
read/write BigQuery data from/to this project.

**Dataset**: Dataset the table belongs to. A dataset is contained within a specific project.
Datasets are top-level containers that are used to organize and control access to tables and views.

**Temporary Bucket Name**: Google Cloud Storage bucket to store temporary data in.
It will be automatically created if it does not exist, but will not be automatically deleted.
Temporary data will be deleted after it is loaded into BigQuery. If it is not provided, a unique
bucket will be created and then deleted after the run finishes.

**Location**: The location where the big query dataset will get created. This value is ignored
if the dataset or temporary bucket already exist.

**Encryption Key Name**: Used to encrypt data written to any bucket, dataset, or table created by the plugin.
If the bucket, dataset, or table already exists, this is ignored.

**Retain BigQuery tables after completion**: By Default, the temporary BigQuery tables used to execute operations
will be deleted after execution is completed. Use this setting to override the default behavior and retain tables.
This can be useful when testing/validating pipelines.

**Temporary Table TTL (in Hours)**: Temporary tables are created with a default TTL. This is used as a safety mechanism
in case the pipeline is interrupted abruptly, and the cleanup process is not completed. Default value is 72 hours.

**Job Priority**: Job Priority used to execute BigQuery jobs (such as Join operations). The value must be 'batch' or 'interactive'. An interactive query is executed
as soon as possible and counts towards the concurrent rate limit and the daily rate limit. A batch query is
queued and started as soon as idle resources are available, usually within a few minutes. If the query hasn't
started within 3 hours, its priority is changed to 'interactive'.

**Use BigQuery Storage Read API**: The [BigQuery Storage Read API](https://cloud.google.com/bigquery/docs/reference/storage) 
can be used to speed up the process to read records from BigQuery into Spark once the execution in BigQuery has 
completed. This API can be used if the execution environment for this environment has **Scala 2.12** installed.
Note that this API has an on-demand price model. See the [Pricing](https://cloud.google.com/bigquery/pricing#storage-api) 
page for details related to pricing.

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

| CDAP type      | BigQuery type                               |
|----------------|---------------------------------------------|
| array          | repeated                                    |
| boolean        | bool                                        |
| bytes          | bytes                                       |
| date           | date                                        |
| datetime       | datetime, string                            |
| decimal        | numeric                                     |
| double / float | float64                                     |
| enum           | unsupported                                 |
| int / long     | int64                                       |
| map            | unsupported                                 |
| record         | struct                                      |
| string         | string, datetime(Should be ISO 8601 format) |
| time           | time                                        |
| timestamp      | timestamp                                   |
| union          | unsupported                                 |

If any of the stages involved in a Join operation contains an unsupported type, 
this Join operation will be executed in Spark.

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
