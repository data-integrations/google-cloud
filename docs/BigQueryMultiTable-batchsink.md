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

**Project ID:** Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console. This is the project
that the BigQuery job will run in. If a temporary bucket needs to be created, the service account
must have permission in this project to create buckets.

**Dataset:** Dataset the tables belongs to. A dataset is contained within a specific project.
Datasets are top-level containers that are used to organize and control access to tables and views.
If dataset does not exist, it will be created.

**Temporary Bucket Name:** Google Cloud Storage bucket to store temporary data in.
It will be automatically created if it does not exist, but will not be automatically deleted.
Temporary data will be deleted after it is loaded into BigQuery. If it is not provided, a unique
bucket will be created and then deleted after the run finishes.

**GCS Upload Request Chunk Size**: GCS upload request chunk size in bytes. Default value is 8388608 bytes.

**Truncate Table:** Whether or not to truncate the table before writing to it.
Should only be used with the Insert operation.

**Location:** The location where the big query datasets will get created. This value is ignored
if the dataset or temporary bucket already exist.

**Split Field:** The name of the field that will be used to determine which table to write to.

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

**Service Account File Path:** Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

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
