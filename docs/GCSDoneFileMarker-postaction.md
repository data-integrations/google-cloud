# Google Cloud Storage Done File Marker

Description
-----------
This post-action plugin marks the end of a pipeline run by creating and storing an empty DONE (or SUCCESS) file in the 
given GCS bucket upon a pipeline *completion*, *success*, or *failure*.

This post-action plugin helps orchestrate various jobs which execution of one job is dependent on another job run 
status.

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
**Run Condition:** When to run the action. Must be *completion*, *success*, or *failure*. Defaults to *completion*.
If set to *completion*, the action will be executed and a marker file will get created regardless of whether the
 pipeline run succeeded or failed.
If set to *success*, the action will get executed and the marker file will get created only if the pipeline run
 succeeded.
If set to *failure*, the action will get executed and the marker file will get created only if the pipeline run
 failed.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Path** Google Cloud Storage path to the marker file. This takes the format `gs://<bucket>/directory/marker-file-name`. 
For example `gs://billing-data/2021-01-21/__SUCCESS`. The marker file will get created only if there is no previous
marker file in that path. Otherwise, creating a new marker file will be skipped. If the bucket does not exist, it
will get created automatically.

* **Service Account**  Service account key used for authorization

* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**JSON**: Contents of the service account JSON file.

**Location:** The location where the GCS bucket will get created.
This value is ignored if the bucket already exists.

**Encryption Key Name**: Used to encrypt data written to any bucket created by the plugin.
If the bucket already exists, this is ignored.

Example
-------
Suppose you want to copy an object from `bucketX` to `bucketY`. Upon a successful copy, you want to mark the process as 
done by creating an empty DONE file in `bucketY`.  

```
bucketX:
    |___catalog.xml
    |___stats.xml
    |___listings/2020-10-10/listings1.csv
    |___listings/2020-10-10/listings2.csv
```  

Suppose that the copying process is done successfully. The `bucketY` contents will show:

```
bucketY:
    |___catalog.xml
    |___stats.xml
    |___listings/2020-10-10/listings1.csv
    |___listings/2020-10-10/listings2.csv
    |___DONE
```  
