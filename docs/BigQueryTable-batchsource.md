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

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Dataset**: Dataset the table belongs to. A dataset is contained within a specific project.
Datasets are top-level containers that are used to organize and control access to tables and views.

**Table**: Table to read from. A table contains individual records organized in rows.
Each record is composed of columns (also called fields).
Every table is defined by a schema that describes the column names, data types, and other information.

**Temporary Bucket Name**: Google Cloud Storage bucket to store temporary data in.
It will be automatically created if it does not exist, but will not be automatically deleted.
Temporary data will be deleted after it has been read. If it is not provided, a unique bucket will be
created and then deleted after the run finishes.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Schema**: Schema of the table to read. This can be fetched by clicking the 'Get Schema' button.
