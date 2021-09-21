# Google Cloud Storage Sink

Description
-----------
This plugin writes records to one or more files in a directory on Google Cloud Storage.
Files can be written in various formats such as csv, avro, parquet, and json.

Cloud Storage allows world-wide storage and retrieval of any amount of data at any time.
You can use Cloud Storage for a range of scenarios including serving website content,
storing data for archival and disaster recovery,
or distributing large data objects to users via direct download.

Credentials
-------------
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

**Path:** Path to write to. For example, gs://<bucket>/path/to/

**Path Suffix:** Time format for the output directory that will be appended to the path.
For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'.
If not specified, nothing will be appended to the path."

**Format:** Format to write the records in.
The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', 'delimited', or the
name of any format plugin that you have deployed to your environment.
If the format is a macro, only the pre-packaged formats can be used.

**Delimiter:** Delimiter to use if the format is 'delimited'.
The delimiter will be ignored if the format is anything other than 'delimited'.

**Write Header:** Whether to write a header to each file if the format is 'delimited', 'csv', or 'tsv'.

**Location:** The location where the gcs bucket will get created. This value is ignored if the bucket already exists.

**Encryption Key Name:** It is used to encrypt data written to any bucket created by the plugin.
If the bucket already exists, this is ignored.

**Content Type:** The Content Type entity is used to indicate the media type of the resource.
Defaults to 'application/octet-stream'. The following table shows valid content types for each format.

| Format type   | Content type                                                                               |
|---------------|--------------------------------------------------------------------------------------------|
| avro          | application/avro, application/octet-stream                                                 |
| csv           | text/csv, application/csv, text/plain, application/octet-stream                            |
| delimited     | text/csv, application/csv, text/tab-separated-values, text/plain, application/octet-stream |
| json          | application/json, text/plain, application/octet-stream                                     |
| orc           | application/octet-stream                                                                   |
| parquet       | application/octet-stream                                                                   |
| tsv           | text/tab-separated-values, text/plain, application/octet-stream                            |

**Custom Content Type:** The Custom Content Type is used when the value of Content-Type is set to other.
User can provide specific Content-Type, different from the options in the dropdown.
More information about the Content-Type can be found at https://cloud.google.com/storage/docs/metadata

**Service Account**  - service account key used for authorization

* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

* **JSON**: Contents of the service account JSON file.

**Output File Prefix:** Prefix for the output file name.  
If none is given, it will default to 'part', which means all data files written by the sink will look like 
'part-r-00000', 'part-r-00001', etc.

**File System Properties:** Additional properties to use with the OutputFormat.

**Schema:** Schema of the data to write.
The 'avro' and 'parquet' formats require a schema but other formats do not.
