
#### **Description**

Read from Cloud Spanner table.

#### **Properties**

Following are properties used to configure this plugin

* **Reference Name**

  Name used to uniquely identify this source for lineage, annotating metadata, etc.

* **Instance Id**

  Instance the Spanner database belongs to. Spanner instance is contained within a specific project.
 Instance is an allocation of resources that is used by Cloud Spanner databases created in that instance.

* **Database Name**

  Database the Spanner table belongs to.
Spanner database is contained within a specific Spanner instance.

* **Table Name**

  Table to read from. A table contains individual records organized in rows.
Each record is composed of columns (also called fields).
Every table is defined by a schema that describes the column names, data types, and other information.

* **Max Partitions**

  Maximum number of partitions. This may be set to the number of map tasks desired.
The maximum value is currently 200,000. This is only a hint.
The actual number of partitions returned may be smaller or larger than this maximum count request.

* **Partition Size (Megabytes)**

  Partition size in megabytes. The desired data size for each partition generated.
This is only a hint. The actual size of each partition may be smaller or larger than this size request.
More information about partition options can be found at https://cloud.google.com/spanner/docs/reference/rest/v1/PartitionOptions

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
