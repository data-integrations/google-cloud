# Google Cloud Storage Parquet File Sink

Description
-----------
This plugin writes records to one or more Parquet files in a directory on Google Cloud Storage.

Cloud Storage allows world-wide storage and retrieval of any amount of data at any time.
You can use Cloud Storage for a range of scenarios including serving website content,
storing data for archival and disaster recovery,
or distributing large data objects to users via direct download.

Authorization
-------------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be provided.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access BigQuery and Google Cloud Storage.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Reference Name:** This will be used to uniquely identify this sink for lineage, annotating metadata, etc.

**Project ID**: The Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Does not need to be specified when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Bucket Name**: The bucket to write to.

**Base Path:** The path to write to. For example, gs://<bucket>/path/to/directory

**Path Suffix:** The time format for the output directory that will be appended to the path.
For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'.
If not specified, nothing will be appended to the path."

**Schema:** The schema of records to write.

**Codec:** The codec to use when writing data. Must be 'none', 'snappy', 'gzip', or 'lzo'. Defaults to 'none.'
