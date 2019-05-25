
#### **Description**

Write to Cloud Spanner table.

#### **Properties**

Following are properties used to configure this plugin

* **Reference Name**

  Name used to uniquely identify this sink for lineage, annotating metadata, etc.

* **Instance Id**

  Instance the Spanner database belongs to. Spanner instance is contained within a specific project.
 Instance is an allocation of resources that is used by Cloud Spanner databases created in that instance.

* **Database Name**

  Database the Spanner table belongs to.
Spanner database is contained within a specific Spanner instance. If the database does not exist, it will get created.

* **Table Name**

  Table to write to. A table contains individual records organized in rows.
Each record is composed of columns (also called fields).
Every table is defined by a schema that describes the column names, data types, and other information.
If the table does not exist, it will get created.

* **Primary Key**

  If the table does not exist, a primary key must be provided in order to auto-create the table.
The key can be a composite key of multiple fields in the schema. This is not required if the table already exists.

* **Write Batch Size**

  Size (in number of records) of the batched writes to the Spanner table.
Each write to Cloud Spanner contains some overhead. To maximize bulk write throughput,
maximize the amount of data stored per write. A good technique is for each commit to mutate hundreds of rows.
Commits with the number of mutations in the range of 1 MiB - 5 MiB rows usually provide the best performance.
Default value is 100 mutations.

* **Schema**

  Schema of the data to write. Must be compatible with the table schema.

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

