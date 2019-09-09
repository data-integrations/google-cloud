# Google Cloud Datastore Source

Description
-----------
This source reads data from Google Cloud Datastore (Datastore mode).
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

**Namespace:** Namespace of the entities to read. A namespace partitions entities into a subset of Cloud Datastore. 
If no value is provided, the `[default]` namespace will be used.

**Kind:** Kind of entities to read. Kinds are used to categorize entities in Cloud Datastore. 
A kind is equivalent to the relational database table notion.

**Ancestor:** Ancestor of entities to read. An ancestor identifies the common parent entity 
that all the child entities share. The value must be provided in key literal format:
`key(kind_1, identifier_1, kind_2, identifier_2, [...])`. For example: `key(kind_1, 'stringId', kind_2, 100)`.

**Filters:** List of filters to apply when reading entities from Cloud Datastore. 
Only entities that satisfy all the filters will be read. The filter key corresponds to a field in the schema. 
The filter value indicates what value that field must have in order to be read. 
If no value is provided, it means the value must be null in order to be read.
TIMESTAMP string should be in the RFC 3339 format without the timezone offset (always ends in Z). 
Expected pattern: `yyyy-MM-dd'T'HH:mm:ssX`, for example: `2011-10-02T13:12:55Z`. 

**Number of Splits:** Desired number of splits to divide the query into when reading from Cloud Datastore. 
Fewer splits may be created if the query cannot be divided into the desired number of splits.

**Key Type:** Type of entity key read from the Cloud Datastore. The type can be one of three values: 

`None` - key will not be included.
 
`Key literal` - key will be included in Cloud Datastore key literal format including complete path with ancestors.

`URL-safe key` - key will be included in the encoded form that can be used as part of a URL. 

Note, if `Key literal` or `URL-safe key` is selected, default key name (`__key__`) or its alias must be present 
in the schema with non-nullable STRING type. 

**Key Alias:** Name of the field to set as the key field. This value is ignored if the `Key Type` is set to `None`. 
If no value is provided, `__key__` is used.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to `auto-detect` when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.

**Schema**: Schema of the data to read. Can be imported or fetched by clicking the `Get Schema` button.

Examples
--------
***Example 1:*** Read entities with filter and key type `None` from Cloud Datastore.

*Initial data in Cloud Datastore `Namespace: sample-ns`, `Kind: User`*

|       Name/ID       | lastName |  company  |
| ------------------- | -------- | --------- |
| id=4505323922522112 | Smith    | Microsoft |
| id=4505323922522113 | Jones    | Google    |
| id=4505323922522114 | Miller   | Microsoft |

*Source Properties*

|   Name    |       Value       |
| --------- | ----------------- |
| Project   | sample-project    |
| Namespace | sample-ns         |
| Kind      | User              |
| Filters   | company|Microsoft |
| Key Type  | None              |

*Output Schema*

|   Name   |  Type  |
| -------- | ------ |
| lastName | STRING |
| company  | STRING |

*Output dataset*

| lastName |  company  |
| -------- | --------- |
| Smith    | Microsoft |
| Miller   | Microsoft |

***Example 2:*** Read entities by `Ancestor` with `Key Alias` and key type `Key literal` from Cloud Datastore.

*Initial data in Cloud Datastore `Namespace: sample-ns`, `Kind: User`*

|    Name/ID    |       Parent        | lastName |  company  |
| ------------- | ------------------- | -------- | --------- |
| name=user-100 | Key(Country, 'USA') | Smith    | Apple     |
| name=user-101 | Key(Country, 'UK')  | Jones    | Amazon    |
| name=user-102 | -                   | Miller   | Microsoft |
| name=user-103 | Key(Country, 'USA') | Wilson   | Facebook  |

*Source Properties*

|   Name    |        Value        |
| --------- | ------------------- |
| Project   | sample-project      |
| Namespace | sample-ns           |
| Kind      | User                |
| Ancestor  | Key(Country, 'USA') |
| Key Type  | Key literal         |
| Key Alias | key                 |

*Output Schema*

|   Name   |  Type  |
| -------- | ------ |
| key      | STRING |
| lastName | STRING |
| company  | STRING |

*Output dataset*

|                  key                  | lastName | company  |
| ------------------------------------- | -------- | -------- |
| Key(Country, 'USA', User, 'user-100') | Smith    | Apple    |
| Key(Country, 'USA', User, 'user-103') | Wilson   | Facebook |

    