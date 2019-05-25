
#### **Description**

Write multiple files on GCS.

#### **Usage**
This plugin is normally used in conjunction with the MultiTableDatabase batch source to write records from multiple
databases into multiple directories in various formats. The plugin expects that the directories it needs to write to
will be set as pipeline arguments, where the key is 'multisink.[directory]' and the value is the schema of the data.

Normally, you rely on the MultiTableDatabase source to set those pipeline arguments, but they can also be manually
set or set by an Action plugin in your pipeline. The sink will expect each record to contain a special split field
that will be used to determine which records are written to each directory. For example, suppose the
the split field is 'tablename'. A record whose 'tablename' field is set to 'activity' will be written to the 'activity'
directory.

This plugin writes records to one or more Avro, ORC, Parquet or Delimited format files in a directory on
Google Cloud Storage.

Cloud Storage allows world-wide storage and retrieval of any amount of data at any time.
You can use Cloud Storage for a range of scenarios including serving website content,
storing data for archival and disaster recovery,or distributing large data objects to users via direct download.

#### **Properties**

Following are properties used to configure this plugin

* **Reference Name**

  This along with the table name will be used to uniquely identify this sink for lineage,
annotating metadata, etc.

* **Path**

  The path to write to. For example, gs://<bucket>/path/to/directory

* **Path Suffix**

  The time format for the output directory that will be appended to the path.
For example, the format 'yyyy-MM-dd-HH-mm' will result in a directory of the form '2015-01-01-20-42'.
If not specified, nothing will be appended to the path."

* **Format**

  Format to write the records in.
The format must be one of 'avro', 'parquet', 'orc' or 'delimited'.

* **Delimiter**

  Delimiter to use if the format is 'delimited'.
The delimiter will be ignored if the format is anything other than 'delimited'.

* **Codec**

  The codec to use when writing data. Must be 'none', 'snappy', 'gzip' or 'deflate', defaults to 'none'.
The 'avro' supports 'snappy' and 'deflate'. The parquet supports 'snappy' and 'gzip'. 
Other formats does not support compression.

* **Split Field**

  The name of the field that will be used to determine which directory to write to.
Defaults to 'tablename'.

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

#### **Example**

Suppose the input records are:

| id  | name     | email            | tablename |
| --- | -------- | ---------------- | --------- |
| 0   | Samuel   | sjax@example.net | accounts  |
| 1   | Alice    | a@example.net    | accounts  |


| userid | item     | action | tablename |
| ------ | -------- | ------ | --------- |
| 0      | shirt123 | view   | activity  |
| 0      | carxyz   | view   | activity  |
| 0      | shirt123 | buy    | activity  |
| 0      | coffee   | view   | activity  |
| 1      | cola     | buy    | activity  |

The plugin will expect two pipeline arguments to tell it to write the first two records to an 'accounts' directory and
the last records to an 'activity' directory.
