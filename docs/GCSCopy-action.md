# Google Cloud Storage Copy

Description
-----------
This plugin copies objects from one Google Cloud Storage bucket to another.
A single object can be copied, or a directory of objects can be copied.

Objects are not copied in parallel, but one by one.

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
**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Source Path**: Path to a source object or directory.

**Destination Path**: Path to the destination.

**Copy All Subdirectories**: If the source is a directory, copy all subdirectories.

**Overwrite Existing Files**: Whether to overwrite existing files during the copy. If this is set to
false and an existing file would be overwritten, the pipeline will fail. This setting does not
protect against race conditions. If a file is written to the destination while this plugin is
running, that file may still get overwritten.

**Service Account**  - service account key used for authorization

* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

* **JSON**: Contents of the service account JSON file.

**Location:** The location where the destination gcs bucket will get created. 
This value is ignored if the bucket already exists.

**Encryption Key Name:** Used to encrypt data written to any bucket created by the plugin.
If the bucket already exists, this is ignored.

Example
-------

Suppose we want to copy objects from 'bucketX' to 'bucketY'. 'bucketY' is completely empty and the
following objects exist in 'bucketX':

    bucketX/catalog.xml
    bucketX/stats.xml
    bucketX/listings/2018-01-01/listings1.csv
    bucketX/listings/2018-01-01/listings2.csv
    bucketX/listings/2018-01-01/listings3.csv
    bucketX/listings/2018-02-01/listings1.csv
    bucketX/listings/2018-02-01/listings2.csv

If the source path is 'bucketX', the destination path is 'bucketY',
and subdirectories are not copied, 'bucketY' will contain:

    bucketY/catalog.xml
    bucketY/stats.xml

If the source path is 'bucketX/listings', the destination path is 'bucketY/dest',
and subdirectories are copied, the 'listings' directory will be copied as the 'dest'
directory' and 'bucketY' will contain:

    bucketY/dest/2018-01-01/listings1.csv
    bucketY/dest/2018-01-01/listings2.csv
    bucketY/dest/2018-01-01/listings3.csv
    bucketY/dest/2018-02-01/listings1.csv
    bucketY/dest/2018-02-01/listings2.csv

However, if the 'dest' directory already exists in 'bucketY', the 'listings' directory
will be copied into the existing 'dest' directory, so 'bucketY' will contain:

    bucketY/dest/listings/2018-01-01/listings1.csv
    bucketY/dest/listings/2018-01-01/listings2.csv
    bucketY/dest/listings/2018-01-01/listings3.csv
    bucketY/dest/listings/2018-02-01/listings1.csv
    bucketY/dest/listings/2018-02-01/listings2.csv
