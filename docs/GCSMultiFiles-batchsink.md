# Google Cloud Storage Multi Files Sink

Description
-----------


This plugin is normally used in conjunction with the MultiTableDatabase batch source to write records from multiple
databases into multiple filesets in text format. Each fileset it writes to will contain a single 'ingesttime' partition,
which will contain the logical start time of the pipeline run. The plugin expects that the filsets it needs to write
to will be set as pipeline arguments, where the key is 'multisink.[fileset]' and the value is the fileset schema.
Normally, you rely on the MultiTableDatabase source to set those pipeline arguments, but they can also be manually
set or set by an Action plugin in your pipeline. The sink will expect each record to contain a special split field
that will be used to determine which records are written to each fileset. For example, suppose the
the split field is 'tablename'. A record whose 'tablename' field is set to 'activity' will be written to the 'activity'
fileset.

This plugin writes records to one or more Avro, ORC, Parquet or Text format files in a directory on Google Cloud Storage.

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

**Codec:** The codec to use when writing data. Must be 'none', 'snappy', or 'deflate'. Defaults to 'none'.


**splitField:** The name of the field that will be used to determine which fileset to write to. Defaults to 'tablename'.

**delimiter:** The delimiter used to separate record fields. Defaults to the tab character..

**outputFormat** The output format Avro, Orc, Parquet or Text. Default value is Text

Example
-------

This example uses a comma to delimit record fields:

    {
        "name": "DynamicMultiFileset",
        "type": "batchsink",
        "properties": {
            "delimiter": ","
        }
    }

Suppose the input records are:

    +-----+----------+------------------+-----------+
    | id  | name     | email            | tablename |
    +-----+----------+------------------+-----------+
    | 0   | Samuel   | sjax@example.net | accounts  |
    | 1   | Alice    | a@example.net    | accounts  |
    +-----+----------+------------------+-----------+
    +--------+----------+--------+-----------+
    | userid | item     | action | tablename |
    +--------+----------+--------+-----------+
    | 0      | shirt123 | view   | activity  |
    | 0      | carxyz   | view   | activity  |
    | 0      | shirt123 | buy    | activity  |
    | 0      | coffee   | view   | activity  |
    | 1      | cola     | buy    | activity  |
    +--------+----------+--------+-----------+

The plugin will expect two pipeline arguments to tell it to write the first two records to an 'accounts' fileset and
the last records to an 'activity' fileset:

    multisink.accounts =
     {
       "type": "record",
       "name": "accounts",
       "fields": [
         { "name": "id", "type": "long" } ,
         { "name": "name", "type": "string" },
         { "name": "email", "type": [ "string", "null" ] }
       ]
     }
    multisink.activity =
     {
       "type": "record",
       "name": "activity",
       "fields": [
         { "name": "userid", "type": "long" } ,
         { "name": "item", "type": "string" },
         { "name": "action", "type": "string" }
       ]
     }