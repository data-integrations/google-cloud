# Google Cloud Storage File Reader

Description
-----------
This plugin reads objects from a path in a Google Cloud Storage bucket.

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
**Reference Name:** This will be used to uniquely identify this source for lineage, annotating metadata, etc.

**Project ID**: The Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Does not need to be specified when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Bucket Name**: The bucket to read from.

**Path:** Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'.
For example, gs://<bucket>/path/to/directory/

**Regex Path Filter:** Regex to filter out files in the path. It accepts regular expression which is applied to the complete
path and returns the list of files that match the specified pattern.

**Path Field:** If specified, each output record will include a field with this name that contains the file URI
that the record was read from.

**Filename Only:** If true and a pathField is specified, only the filename will be used.
If false, the full URI will be used. Defaults to false.

**Schema:** Output schema. If a Path Field is set, it must be present in the schema as a string.

**maxSplitSize:** Maximum split-size for each mapper in the MapReduce Job. Defaults to 128MB. (Macro-enabled)

**Ignore Non-Existing Folders:** Identify if path needs to be ignored or not, for case when directory or file does not
exists. If set to true it will treat the not present folder as 0 input and log a warning. Default is false.

**Read Files Recursively:** Whether files are to be read recursively from the path. Default is false.
