# Google Cloud BigQuery Delete

Description
-----------
This plugin deletes BigQuery dataset or tables.
BigQuery is Google's serverless, highly scalable, enterprise data warehouse.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access BigQuery.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Project ID**: The Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Dataset Project ID**: Project the dataset or table belongs to. This is only required if the dataset or table is not
in the same project that the BigQuery query will run in. If no value is given,
it will default to the configured Project ID.

**Dataset Name**: Name of the dataset to delete or if a table needs to be deleted then the name of the dataset in
which the table exists.

**Dataset Name**: Name of the table to be deleted.

**Delete Contents** Specifies whether to delete a dataset even when it is not empty. Defaults to false. This value is
ignored while deleting a table.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.
