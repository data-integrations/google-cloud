# Google Cloud Bigtable Sink

Description
-----------
This sink writes data to Google Cloud Bigtable.
Cloud Bigtable is Google's NoSQL Big Data database service. 
It's the same database that powers many core Google services, including Search, Analytics, Maps, and Gmail.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access Bigtable.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Instance ID**: Google Cloud Bigtable instance ID.

**Table**: Database table name.

**Key Alias**: Name of the field for row key.

**Column Mappings**: Mappings from record field to Bigtable column name. 
Column names must be formatted as 'family:qualifier'.

**Bigtable Options**: Additional connection properties for Bigtable.
Full list of allowed properties: https://cloud.google.com/bigtable/docs/hbase-client/javadoc/constant-values.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.
