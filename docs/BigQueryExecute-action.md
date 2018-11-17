# Google Cloud BigQuery Execute

Description
-----------
This plugin executes a BigQuery SQL query.
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

**SQL**: The SQL command to execute.

**Legacy**: Specified whether to use BigQuery's legacy SQL dialect for this query. By default this property is
set to 'false'. If set to 'false', the query will use BigQuery's Standard SQL.

**Mode**: Mode to execute the query in. The value must be 'batch' or 'interactive'. A batch query is executed as
soon as possible and count towards the concurrent rate limit and the daily rate limit. An interactive query is
queued and started as soon as idle resources are available, usually within a few minutes. If the query hasn't
started within 3 hours, its priority is changed to 'INTERACTIVE'.

**Dataset Name**: The dataset to store the query results in. If not specified, the results will not be stored.

**Table Name**: The table to store the query results in. If not specified, the results will not be stored.

**Cache**: Specifies whether to use cache when executing the query.

**Job Location**: Location of the job. Must match the location of the dataset specified in the query. Defaults to 'US'

**Timeout**: Query timeout in minutes. Defaults to 10.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.
