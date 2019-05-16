
#### **Description**

Move a GCS object or directory of objects from source to destination.

#### **Properties**

Following are properties used to configure this plugin

* **Source Path**

  Path to a source object or directory.

* **Destination Path**

  Path to the destination. The bucket must already exist.

* **Copy All Subdirectories**

  If the source is a directory, move all subdirectories.

* **Overwrite Existing Files**

  Whether to overwrite existing files during the move. If this is set to
false and an existing file would be overwritten, the pipeline will fail. This setting does not
protect against race conditions. If a file is written to the destination while this plugin is
running, that file may still get overwritten.

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

#### **Example**

Suppose we want to move objects from 'bucketX' to 'bucketY'. 'bucketY' is completely empty and the
following objects exist in 'bucketX':

    bucketX/catalog.xml
    bucketX/stats.xml
    bucketX/listings/2018-01-01/listings1.csv
    bucketX/listings/2018-01-01/listings2.csv
    bucketX/listings/2018-01-01/listings3.csv
    bucketX/listings/2018-02-01/listings1.csv
    bucketX/listings/2018-02-01/listings2.csv

If the source path is 'bucketX', the destination path is 'bucketY',
and subdirectories are not moved, 'bucketY' will contain files:

    bucketY/catalog.xml
    bucketY/stats.xml

and 'bucketX' will contain files:

    bucketX/listings/2018-01-01/listings1.csv
    bucketX/listings/2018-01-01/listings2.csv
    bucketX/listings/2018-01-01/listings3.csv
    bucketX/listings/2018-02-01/listings1.csv
    bucketX/listings/2018-02-01/listings2.csv

If the source path is 'bucketX/listings', the destination path is 'bucketY/dest',
and subdirectories are copied, the 'listings' directory will be copied as the 'dest'
directory' and 'bucketY' will contain:

    bucketY/dest/2018-01-01/listings1.csv
    bucketY/dest/2018-01-01/listings2.csv
    bucketY/dest/2018-01-01/listings3.csv
    bucketY/dest/2018-02-01/listings1.csv
    bucketY/dest/2018-02-01/listings2.csv

and 'bucketX' will contain:

    bucketX/catalog.xml
    bucketX/stats.xml

However, if the 'dest' directory already exists in 'bucketY', the 'listings' directory
will be copied into the existing 'dest' directory, so 'bucketY' will contain:

    bucketY/dest/listings/2018-01-01/listings1.csv
    bucketY/dest/listings/2018-01-01/listings2.csv
    bucketY/dest/listings/2018-01-01/listings3.csv
    bucketY/dest/listings/2018-02-01/listings1.csv
    bucketY/dest/listings/2018-02-01/listings2.csv

and 'bucketX' will contain:

    bucketX/catalog.xml
    bucketX/stats.xml