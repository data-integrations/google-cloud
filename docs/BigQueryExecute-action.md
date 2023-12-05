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
**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.
This is the project that the BigQuery SQL will run in.

**Dataset Project ID**: Project ID of the dataset to store the query results in. This is only required if the 
dataset is not in the same project that the BigQuery job will run in. If no value is given, it will default to the
configured Project ID. `BigQuery Data Editor` role on this project must be granted to the specified service account to
write BigQuery data to this project.

**SQL**: SQL command to execute.

**BQ Job Labels:** Key value pairs to be added as labels to the BigQuery job. Keys must be unique. (Macro Enabled)

[job_source, type] are system defined labels used by CDAP for internal purpose and cannot be used as label keys.
Macro format is supported. example `key1:val1,key2:val2`

Keys and values can contain only lowercase letters, numeric characters, underscores, and dashes.
For more information about labels, see [Docs](https://cloud.google.com/bigquery/docs/labels-intro#requirements).

**Dialect**: Dialect of the SQL command. The value must be 'legacy' or 'standard'. If set to 'standard',
the query will use BigQuery's standard SQL: https://cloud.google.com/bigquery/sql-reference/.
If set to 'legacy', BigQuery's legacy SQL dialect will be used for this query.

**Mode**: Mode to execute the query in. The value must be 'batch' or 'interactive'. An interactive query is executed 
as soon as possible and counts towards the concurrent rate limit and the daily rate limit. A batch query is
queued and started as soon as idle resources are available, usually within a few minutes. If the query hasn't
started within 3 hours, its priority is changed to 'interactive'.

**Dataset Name**: Dataset to store the query results in. If not specified, the results will not be stored.

**Table Name**: Table to store the query results in. If not specified, the results will not be stored.

**Use Cache**: Specifies whether to look for the result in the query cache. The query cache is a best-effort
cache that will be flushed whenever tables in the query are modified.

**Job Location**: Location of the job. It must match the location of the dataset specified in the query.

**Store results in a BigQuery Table**: Whether to store results in a BigQuery Table.

**Encryption Key Name**: Used to encrypt data written to the dataset or table created by the plugin to store the query results.
It is only applicable when users choose to store the query results in a BigQuery table.
More information can be found [here](https://cloud.google.com/data-fusion/docs/how-to/customer-managed-encryption-keys)

**Row As Arguments**: Row as arguments. For example, if the query is 'select min(id) as min_id, max(id) as max_id from my_dataset.my_table',
an arguments for 'min_id' and 'max_id' will be set based on the query results. Plugins further down the pipeline can then
reference these values with macros ${min_id} and ${max_id}.

**Service Account**  - service account key used for authorization

* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

* **JSON**: Contents of the service account JSON file.
