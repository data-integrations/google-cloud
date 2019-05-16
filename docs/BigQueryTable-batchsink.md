
#### **Description**

Write to a BigQuery table.

#### **Usage**

Data is first written to a temporary location on GCS, then loaded into BigQuery from there.

#### **Properties**

Following are properties used to configure this plugin

* **Reference Name**

  Name used to uniquely identify this sink for lineage, annotating metadata, etc.

* **Dataset**

  Dataset the table belongs to. A dataset is contained within a specific project.
Datasets are top-level containers that are used to organize and control access to tables and views.

* **Table**

  Table to write to. A table contains individual records organized in rows.
Each record is composed of columns (also called fields).
Every table is defined by a schema that describes the column names, data types, and other information.

* **Temporary Bucket Name**

  Google Cloud Storage bucket to store temporary data in.
It will be automatically created if it does not exist, but will not be automatically deleted.
Temporary data will be deleted after it is loaded into BigQuery. If it is not provided, a unique
bucket will be created and then deleted after the run finishes.


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
