# Google Cloud Storage File Reader

Description
-----------
This plugin reads objects from a path in a Google Cloud Storage bucket.

Cloud Storage allows world-wide storage and retrieval of any amount of data at any time.
You can use Cloud Storage for a range of scenarios including serving website content,
storing data for archival and disaster recovery,
or distributing large data objects to users via direct download.

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

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Bucket Name**: Bucket to read from.

**Path:** Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'.
For example, gs://<bucket>/path/to/directory/.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Maximum Split Size:** Maximum split-size for each mapper in the MapReduce Job. Defaults to 128MB.

**Path Field:** Output field that contains the file URI that the record was read from.

**Path Filename Only:** Whether to use just the filename instead of the entire URI when a Path Field is specified.
If false, the full URI will be used.

**Read Files Recursively:** Whether files are to be read recursively from the path. Default is false.

**File System Properties:** Additional properties to use with the InputFormat when reading the data.

**Schema:** Output schema. If a Path Field is set, it must be present in the schema as a string.
