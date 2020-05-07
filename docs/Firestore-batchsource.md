# Google Cloud Firestore Batch Source

Description
-----------
This source reads documents from a Google Cloud Firestore collection. The user can optionally provide an input query 
to filter documents from Firestore. 

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to `auto-detect`. Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access Google Cloud Firestore.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
-------------
**Reference Name**: Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to `auto-detect` when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Database Id**: Firestore database name. If no value is provided, the `(default)` database will be used.

**Collection Name**: Name of the Firestore collection to read data from.

**Mode**: Mode of query. The mode can be one of two values: 

`Basic` - will allow user to specify documents to pull or skip.  

`Advanced` - will allow user to specify custom query.

**Documents to pull**: Specify the document ids to be extracted from Firestore Collection; for example: `'Doc1,Doc2'`. 
Note, This applies only if Mode is set to `Basic`.  

**Documents to skip**: Specify the document ids to be skipped from Firestore Collection; for example: `'Doc1,Doc2'`. 
Note, This applies only if Mode is set to `Basic`.  

**Filters**: Specify the custom filter for fetching documents from Firestore Collection. Supported operators are, 
* EqualTo
* NumericEqualTo
* LessThan
* LessThanOrEqualTo
* GreaterThan
* GreaterThanOrEqualTo

A filter must specify the operator with field it should filter on as well the value.
Filters are specified using syntax: `value:operator(field)[,value:operator(field)]`.

For example, `CA:EqualTo(state),1000000:LessThan(population)` will apply two filters.
The first will create a filter as `state = 'CA'`.
The second will create a filter as `population < 1000000`.
  
Note, This applies only if Mode is set to `Advanced`.  

**Include Document Id**: Option to include Document ID in the output. This can have one two values:

`true` - document id will be included.

`false` - document id will not be included.

Note, if `true` is selected, default document id name (`__id__`) or its alias must be present 
in the schema with non-nullable STRING type. 

**Document Id Alias**: Name of the field to set as the document id field. This value is ignored if the `Include Document Id` is set to `false`. 
If no value is provided, `__id__` is used.

**Schema**: Schema of the data to read. Can be imported or fetched by clicking the `Get Schema` button.

