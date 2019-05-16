
#### **Description**

Read from GCS.

#### **Properties**

Following are properties used to configure this plugin

* **Reference Name**

  Name used to uniquely identify this source for lineage, annotating metadata, etc.

* **Path**

  Path to file(s) to be read. If a directory is specified, terminate the path name with a '/'.
For example, gs://<bucket>/path/to/directory/.

* **Format**

  Format of the data to read.
The format must be one of 'avro', 'blob', 'csv', 'delimited', 'json', 'parquet', 'text', or 'tsv'.
If the format is 'blob', every input file will be read into a separate record.
The 'blob' format also requires a schema that contains a field named 'body' of type 'bytes'.
If the format is 'text', the schema must contain a field named 'body' of type 'string'.

* **Delimiter**

  Delimiter to use when the format is 'delimited'. This will be ignored for other formats.

* **Maximum Split Size**

  Maximum size in bytes for each input partition.
Smaller partitions will increase the level of parallelism, but will require more resources and overhead.
The default value is 128MB.

* **Path Field**

  Output field to place the path of the file that the record was read from.
If not specified, the file path will not be included in output records.
If specified, the field must exist in the output schema as a string.

* **Path Filename Only**

  Whether to only use the filename instead of the URI of the file path when a path field is given.
The default value is false.

* **Read Files Recursively**

  Whether files are to be read recursively from the path. The default value is false.

* **File System Properties**

  Additional properties to use with the InputFormat when reading the data.

* **Schema** Output schema.

  If a Path Field is set, it must be present in the schema as a string.

#### **Credentials**

If the plugin is run in GCP environment, the service account file path does not need to be
specified and can be set to 'auto-detect'. Credentials will be automatically read from the GCP environment.
A path to a service account key must be provided when not running in GCP. The service account
key can be found on the Dashboard in the Cloud Platform Console. Ensure that the account key has permission
to access resource.

* **Project Id**

  Google Cloud Project Id, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

* **Service Account File Path**

  Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running in GCP. When running on outside GCP,
the file must be present on every node were pipeline runs.
