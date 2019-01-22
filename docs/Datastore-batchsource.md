# Google Cloud Datastore Source

Description
-----------
This source reads data from Google Cloud Datastore.
Datastore is a NoSQL document database built for automatic scaling and high performance.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to `auto-detect`. Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access Google Cloud Datastore.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Namespace:** A namespace partitions entities into a subset of datastore. 
If not provided, `[default]` namespace will be used. 

**Kind:** The kind of an entity categorizes it for the purpose of Datastore queries. 
Equivalent to relational database table notion.

**Has Ancestor:** Ancestor identifies the common root entity in which the entities are grouped. 
Must be written in Key Literal format: `key(kind_1, identifier_1, kind_2, identifier_2, [...])`.
Example: `key(kind_1, 'stringId', kind_2, 100)`.

**Filters:** List of filter property names and values pairs by which equality filter will be applied. 
This is a semi-colon separated list of key-value pairs, where each pair is separated by a pipe sign `|`. 
Filter properties must be present in the schema. Allowed property types are STRING, LONG, DOUBLE, BOOLEAN and TIMESTAMP. 
Property value indicated as `null` string will be treated as `is null` clause. 
TIMESTAMP string should be in the RFC 3339 format without the timezone offset (always ends in Z). 
Expected pattern: `yyyy-MM-dd'T'HH:mm:ssX`, example: `2011-10-02T13:12:55Z`. 

**Number of Splits:** Desired number of splits to split a query into multiple shards during execution. 
Will be created up to desired number of splits, however less splits can be created if desired number is unavailable. 

**Key Type:** Key is unique identifier assigned to the entity when it is created. 
Property defines if key will be included in the output, commonly is needed to perform upserts to the Cloud Datastore.
Can be one of three options: 

`None` - key will not be included.
 
`Key literal` - key will be included in Datastore key literal format including complete path with ancestors.

`URL-safe key` - key will be included in the encoded form that can be used as part of a URL. 

Note, if `Key literal` or `URL-safe key` is selected, default key name (`__key__`) or its alias must be present 
in the schema with non-nullable STRING type. 

**Key Alias:** Allows to set user-friendly name for the key column which default name is `__key__`. 
Only applicable, if `Key Type` is set to `Key literal` or `URL-safe key`. 
If `Key Type` is set to `None`, property must be empty.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to `auto-detect` when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Schema**: Schema of the data to read, can be imported or fetched by clicking the `Get Schema` button.

Example
-------
This example connects to Google Cloud Datastore, extracts data from Namespace *Music*, Kind *Rock* 
for the Artist *Pink Floyd*. Datastore key will be included in the dataset in a form of *Key literal* with name *key*. 
If data capacity allows, query will be split into 10 splits.

    {
         "name": "Datastore",
         "type": "batchsource",
         "properties": {
            "referenceName": "RefDatastoreSource",
            "project": "auto-detect",
            "serviceFilePath": "auto-detect",
            "namespace": "Music"
            "kind": "Rock",
            "filters": "Artist|Pink Floyd",
            "numSplits": "10",
            "keyType": "Key literal",
            "keyAlias": "key"
         }
    }
    