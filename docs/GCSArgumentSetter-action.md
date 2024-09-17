# GCS Argument Setter

Description
-----------

Fetch Json File from GCS, to set arguments in the pipeline.

The plugin provides the ability to map json properties as pipeline arguments name and columns
values as pipeline arguments Following is JSON configuration that can be provided.

This is most commonly used when the structure of a pipeline is static,
and its configuration needs to be managed outside the pipeline itself.
   
The Json File must contain arguments in a list:

    {
        "arguments" : [
            { "name" : "argument name", "type" : "type", "value" : "argument value"},
            { "name" : "argument1 name", "type" : "type", "value" : "argument1 value"}
        ]
    }
Where type can be Schema, Int, Float, Double, Short, String, Char, Array, Map 
 
For more examples visit: https://github.com/data-integrations/argument-setter
    
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
It can be found on the Dashboard in the Google Cloud Platform Console. This is the project
that the BigQuery job will run in. If a temporary bucket needs to be created, the service account
must have permission in this project to create buckets.

**Path**: GCS Path to the file containing the arguments.

**Service Account**  - service account key used for authorization

* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

* **JSON**: Contents of the service account JSON file.
