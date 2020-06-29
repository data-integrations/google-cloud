# Google BigQuery Table Source

Description
-----------
Performs an BigQueryTable Query request to fetch arguments to set in the pipeline.

This is most commonly used when the structure of a pipeline is static,
and its configuration needs to be managed outside the pipeline itself.


Arguments name must match column name in BigQueryTable

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
It can be found on the Dashboard in the Google Cloud Platform Console. This is the project
that the BigQuery job will run in. If a temporary bucket needs to be created, the service account
must have permission in this project to create buckets.

**Dataset**: Dataset the table belongs to. A dataset is contained within a specific project.
Datasets are top-level containers that are used to organize and control access to tables and views.

**Table**: Table to read from. A table contains individual records organized in rows.
Each record is composed of columns (also called fields).
Every table is defined by a schema that describes the column names, data types, and other information.

**Argument Selection Conditions**: A set of conditions for identifying the arguments to run a pipeline.
A particular use case for this would be feed=marketing; date=20200427.

**Arguments Columns**:Name of the columns, separated by `,` ,that contains the arguments for this run


**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.


