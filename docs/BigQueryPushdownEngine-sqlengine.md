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

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console. This is the project
that the BigQuery job will run in. If a temporary bucket needs to be created, the service account
must have permission in this project to create buckets.

**Dataset Project**: Project the dataset belongs to. This is only required if the dataset is not
in the same project that the BigQuery job will run in. If no value is given, it will default to the 
configured Project ID.

**Dataset**: Dataset the table belongs to. A dataset is contained within a specific project.
Datasets are top-level containers that are used to organize and control access to tables and views.

**Temporary Bucket Name**: Google Cloud Storage bucket to store temporary data in.
It will be automatically created if it does not exist, but will not be automatically deleted.
Temporary data will be deleted after it is loaded into BigQuery. If it is not provided, a unique
bucket will be created and then deleted after the run finishes.

**Location**: The location where the big query dataset will get created. This value is ignored
if the dataset or temporary bucket already exist.

**Retain BigQuery tables after completion**: By Default, the temporary BigQuery tables used to execute operations
will be deleted after execution is completed. Use this setting to override the default behavior and retain tables.
This can be useful when testing/validating pipelines.

**Temporary Table TTL (in Hours)**: Temporary tables are created with a default TTL. This is used as a safety mechanism
in case the pipeline is interrupted abruptly, and the cleanup process is not completed. Default value is 72 hours.

**Job Priority**: Job Priority used to execute BigQuery jobs (such as Join operations). The value must be 'batch' or 'interactive'. An interactive query is executed
as soon as possible and counts towards the concurrent rate limit and the daily rate limit. A batch query is
queued and started as soon as idle resources are available, usually within a few minutes. If the query hasn't
started within 3 hours, its priority is changed to 'interactive'.

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
