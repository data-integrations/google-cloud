
#### **Description**

Read from Datastore

#### **Properties**

Following are properties used to configure this plugin

* **Reference Name**

 Name used to uniquely identify this source for lineage, annotating metadata, etc.

* **Namespace**

 Namespace of the entities to read. A namespace partitions entities into a subset of Cloud Datastore.
If no value is provided, the `[default]` namespace will be used.

* **Kind**

 Kind of entities to read. Kinds are used to categorize entities in Cloud Datastore.
A kind is equivalent to the relational database table notion.

* **Ancestor**

 Ancestor of entities to read. An ancestor identifies the common parent entity
that all the child entities share. The value must be provided in key literal format:
`key(kind_1, identifier_1, kind_2, identifier_2, [...])`. For example: `key(kind_1, 'stringId', kind_2, 100)`.

* **Filters**

 List of filters to apply when reading entities from Cloud Datastore.
Only entities that satisfy all the filters will be read. The filter key corresponds to a field in the schema. 
The filter value indicates what value that field must have in order to be read. 
If no value is provided, it means the value must be null in order to be read.
TIMESTAMP string should be in the RFC 3339 format without the timezone offset (always ends in Z). 
Expected pattern: `yyyy-MM-dd'T'HH:mm:ssX`, for example: `2011-10-02T13:12:55Z`. 

* **Number of Splits**

 Desired number of splits to divide the query into when reading from Cloud Datastore.
Fewer splits may be created if the query cannot be divided into the desired number of splits.

* **Key Type**

 Type of entity key read from the Cloud Datastore. The type can be one of three values:

   1. `None` - key will not be included.
   2. `Key literal` - key will be included in Cloud Datastore key literal format including complete path with ancestors.
   3. `URL-safe key` - key will be included in the encoded form that can be used as part of a URL.

  Note, if `Key literal` or `URL-safe key` is selected, default key name (`__key__`) or its alias must be present
in the schema with non-nullable STRING type. 

* **Key Alias**

 Name of the field to set as the key field. This value is ignored if the `Key Type` is set to `None`.
If no value is provided, `__key__` is used.

* **Schema**

 Schema of the data to read. Can be imported or fetched by clicking the `Get Schema` button.

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

    