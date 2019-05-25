
#### **Description**

Delete a GCS bucket.

#### **Usage**

This plugin deletes objects in a Google Cloud Storage bucket.
Cloud Storage allows world-wide storage and retrieval of any amount of data at any time.

Buckets are the basic containers that hold your data.
Everything that is stored in Cloud Storage must be contained in a bucket.
Buckets are used to organize and control access to data.

Objects are the individual pieces of data that are stored in Cloud Storage.
Object names can contain any combination of Unicode characters (UTF-8 encoded) and must be less than 1024 bytes in length.
Object names often look like file paths.

#### **Properties**

Following are properties used to configure this plugin

* **Objects to Delete**

  Comma separated list of objects to delete.

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
