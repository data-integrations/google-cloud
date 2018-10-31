# Google Cloud Storage Delete

Description
-----------
This plugin deletes objects in a Google Cloud Storage bucket.
Cloud Storage allows world-wide storage and retrieval of any amount of data at any time.

Buckets are the basic containers that hold your data.
Everything that is stored in Cloud Storage must be contained in a bucket.
Buckets are used to organize and control access to data.

Objects are the individual pieces of data that are stored in Cloud Storage.
Object names can contain any combination of Unicode characters (UTF-8 encoded) and must be less than 1024 bytes in length.
Object names often look like file paths.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access BigQuery and Google Cloud Storage.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Project ID**: The Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Objects to Delete**: Comma separated list of objects to delete.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.
