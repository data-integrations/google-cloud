# Google Dataplex Source

Description
-----------
This source reads from a Dataplex entity. Dataplex is an intelligent data fabric that unifies your distributed data to
help automate data management and power analytics at scale.
Data from the Dataplex entity is first exported to a temporary location on Google Cloud Storage, then read into the
pipeline from there.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be provided and can be
set to 'auto-detect'. Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided. The service account
key can be found on the Dashboard in the Cloud Platform Console. Make sure the account key has permission to access
BigQuery, Google Cloud Storage and Dataplex. The service account key file needs to be available on every node in your
cluster and must be readable by all users running the job.

Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project. It can be found on the Dashboard in the
Google Cloud Platform Console. This is the project that the Dataplex job will run in. If a temporary bucket needs to be
created, the service account must have permission in this project to create buckets.

**Service Account**  - service account key used for authorization

* **File Path**: Path on the local file system of the service account key used for authorization. Can be set to '
  auto-detect' when running on a Dataproc cluster. When running on other clusters, the file must be present on every
  node in the cluster.

* **JSON**: Contents of the service account JSON file.

**Location ID**: ID of the location in which the Dataplex lake has been created, which can be found on the details page
of the lake.

**Lake ID**: ID of the Dataplex lake, which can be found on the details page of the lake.

**Zone ID**: ID of the Dataplex zone, which can be found on the details page of the zone.

**Entity ID**: ID of the Dataplex entity, which can be found on the Discovery tab.

(**Note**: Partition Start Date and Partition End Date are only applicable for BigQuery entities with time
partitioning.)

**Partition Start Date**: Inclusive partition start date, specified as ‘yyyy-MM-dd’. For example, ‘2019-01-01’. If no
value is given, all partitions up to the partition end date will be read.

**Partition End Date**: Exclusive partition end date, specified as ‘yyyy-MM-dd’. For example, ‘2019-01-01’. If no value
is given, all partitions up from the partition start date will be read.

**Filter**: Filters out rows that do not match the given condition. For example, if the filter is ‘age > 50 and name is
not null’, all output rows will have an ‘age’ over 50 and a value for the ‘name’ field. This is the same as the WHERE
clause in BigQuery. More information can be found at
https://cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#where_clause

Data Type Mappings from Dataplex Entity to CDAP
----------
The following table lists out different Dataplex data types, as well as the corresponding CDAP data type for each
Dataplex type.

| Dataplex type | CDAP type                             |
|---------------|---------------------------------------|
| bool          | boolean                               |
| bytes         | bytes                                 |
| date          | date                                  |
| datetime      | datetime(Should be ISO 8601 format)   |
| float64       | double                                |
| geo           | unsupported                           |
| int64         | long                                  |
| numeric       | decimal (38 digits, 9 decimal places) |
| record        | record                                |
| repeated      | array                                 |
| string        | string                                |
| struct        | record                                |
| time          | time (microseconds)                   |
| timestamp     | timestamp (microseconds)              |
