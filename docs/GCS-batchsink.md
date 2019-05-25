
#### **Description**

Write to Google Cloud Storage(GCS)

#### **Usage**

This plugin writes records to one or more files in a directory on Google Cloud Storage.
Files can be written in various formats such as csv, avro, parquet, and json.

#### **Properties**

Following are properties used to configure this plugin

* **Reference Name**

 Name used to uniquely identify this sink for lineage, annotating metadata, etc.

* **Path**

 Path to write to. For example, gs://<bucket>/path/to/

* **Path Suffix**

 Time format for the output directory that will be appended to the path.
For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'.
If not specified, nothing will be appended to the path."

* **Format**

 Format to write the records in. The format must be one of 'json', 'avro', 'parquet', 'csv', 'tsv', or 'delimited'.
The 'avro' and 'parquet' formats require a schema but other formats do not.

* **Delimiter**

 Delimiter to use if the format is 'delimited'.
The delimiter will be ignored if the format is anything other than 'delimited'.


#### **Project and Credentials**

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
