# Google Cloud Datastore Sink

Description
-----------
This sink writes data to Google Cloud Datastore (Datastore mode).
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
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Namespace:** Namespace of the entities to write. A namespace partitions entities into a subset of Cloud Datastore. 
If no value is provided, the `default` namespace will be used. 

**Kind:** Kind of entities to write. Kinds are used to categorize entities in Cloud Datastore. 
A kind is equivalent to the relational database table notion.

**Key Type:** Type of key assigned to entities written to the Cloud Datastore. The type can be one of four values: 

`Auto-generated key` - key will be generated by Cloud Datastore as a _Numeric ID_.

`Custom name` - key will be provided as a field in the input records. The key field must not be nullable and must be 
of type STRING, INT or LONG.

`Key literal` - key will be provided as a field in the input records in key literal format. The key field type must be 
a non-nullable string and the value must be in key literal format: 
`key(<kind>, <identifier>, <kind>, <identifier>, [...])`.
Example: `key(kind_name, 'stringId')` 

`URL-safe key` - key will be provided as a field in the input records in encoded URL form. The key field type must be 
a non-nullable string and the value must be a URL-safe string.

**Key Alias:** The field that will be used as the entity key when writing to Cloud Datastore. This must be provided 
when the Key Type is not auto generated. 

**Ancestor:** Ancestor identifies the common root entity in which the entities are grouped. 
An ancestor must be specified in key literal format: `key(<kind>, <identifier>, <kind>, <identifier>, [...])`.
Example: `key(kind_1, 'stringId', kind_2, 100)` 

**Index Strategy** Defines which fields will be indexed in Cloud Datastore. 
Can be one of three options: 

`All` - all fields will be indexed

`None` - none of fields will be indexed 

`Custom` - indexed fields will be provided in `Indexed Properties`

**Indexed Properties:** Fields to index in Cloud Datastore. A value must be provided if the `Index Strategy` is 
`Custom`, otherwise it is ignored.

**Batch Size:** Maximum number of entities that can be passed in one batch to a Commit operation. 
The minimum value is `1` and maximum value is `500`.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Service Account**  - service account key used for authorization
* **File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.
* **JSON**: Contents of the service account JSON file.

**Use Transactions**: Whether to use transactions or not when committing record batches into Datastore.
See the [Datastore documentation on Transactions](https://cloud.google.com/datastore/docs/concepts/transactions)

Examples
--------
***Example 1:*** Insert new entities with key type `Auto-generated key` to Cloud Datastore.

*Initial state for `Namespace: sample-ns`, `Kind: User`*

***No records.***

*Input Dataset*

| lastName |  company  |
| -------- | --------- |
| Smith    | Apple     |
| Jones    | Google    |
| Miller   | Microsoft |

*Sink Properties*

|   Name    |       Value        |
| --------- | ------------------ |
| Project   | sample-project     |
| Namespace | sample-ns          |
| Kind      | User               |
| Key Type  | Auto-generated key |

*Input Schema*

|   Name   |  Type  |
| -------- | ------ |
| lastName | STRING |
| company  | STRING |

*Output in Cloud Datastore `Namespace: sample-ns`, `Kind: User`*

|       Name/ID       | lastName |  company  |
| ------------------- | -------- | --------- |
| id=4505323922522112 | Smith    | Apple     |
| id=4505323922522113 | Jones    | Google    |
| id=4505323922522114 | Miller   | Microsoft |

***Example 2:*** Insert new entities with `Ancestor` and key type `Custom name` to Cloud Datastore.

*Initial state for `Namespace: sample-ns`, `Kind: User`*

***No records.***

*Input Dataset*

|   key    | lastName |  company  |
| -------- | -------- | --------- |
| user-100 | Smith    | Apple     |
| user-101 | Jones    | Google    |
| user-102 | Miller   | Microsoft |

*Sink Properties*

|   Name    |        Value        |
| --------- | ------------------- |
| Project   | sample-project      |
| Namespace | sample-ns           |
| Kind      | User                |
| Ancestor  | Key(Country, 'USA') |
| Key Type  | Custom name         |
| Key Alias | key                 |

*Input Schema*

|   Name   |  Type  |
| -------- | ------ |
| key      | STRING |
| lastName | STRING |
| company  | STRING |

*Output in Cloud Datastore `Namespace: sample-ns`, `Kind: User`*

|    Name/ID    |       Parent        | lastName |  company  |
| ------------- | ------------------- | -------- | --------- |
| name=user-100 | Key(Country, 'USA') | Smith    | Apple     |
| name=user-101 | Key(Country, 'USA') | Jones    | Google    |
| name=user-102 | Key(Country, 'USA') | Miller   | Microsoft |

***Example 3:*** Upsert entities with new field `isContractor` and key type `Key literal` to Cloud Datastore.

*Initial state for `Namespace: sample-ns`, `Kind: User`*

| Name/ID |       Parent        | lastName | company |
| ------- | ------------------- | -------- | ------- |
| id=1    | Key(Country, 'USA') | Smith    | Apple   |
| id=2    | Key(Country, 'USA') | Jones    | Google  |

*Input Dataset*

|             key              | lastName | company | isContractor |
| ---------------------------- | -------- | ------- | ------------ |
| Key(Country, 'USA', User, 1) | Smith    | Netflix | true         |
| Key(User, 3)                 | Miller   | Google  | false        |

*Sink Properties*

|   Name    |     Value      |
| --------- | -------------- |
| Project   | sample-project |
| Namespace | sample-ns      |
| Kind      | User           |
| Key Type  | Key literal    |
| Key Alias | key            |

*Input Schema*

|     Name     |  Type   |
| ------------ | ------- |
| key          | STRING  |
| lastName     | STRING  |
| company      | STRING  |
| isContractor | BOOLEAN |

*Output in Cloud Datastore `Namespace: sample-ns`, `Kind: User`*

| Name/ID |       Parent        | lastName | company | isContractor |
| ------- | ------------------- | -------- | ------- | ------------ |
| id=1    | Key(Country, 'USA') | Smith    | Netflix | true         |
| id=2    | Key(Country, 'USA') | Jones    | Google  | -            |
| id=3    | -                   | Miller   | Google  | false        |

***Example 4:*** Update entities values without `Key alias` using key type `URL-safe key` in Cloud Datastore.

*Initial state for `Namespace: sample-ns`, `Kind: User`*

|    Name/ID    | lastName | company | isContractor |
| ------------- | -------- | ------- | ------------ |
| name=user-100 | Smith    | Netflix | true         |
| name=user-101 | Jones    | Google  | -            |
| name=user-102 | Miller   | Google  | false        |

*Input Dataset*

|                              __key__                               | lastName | company | isContractor |
| ------------------------------------------------------------------ | -------- | ------- | ------------ |
| partition_id+%7B%0Aproject_id%3A+%22sample-project% … user-100%22% | Smith    | Netflix | false        |
| partition_id+%7B%0Aproject_id%3A+%22sample-project% … user-101%22% | Jones    | Google  | false        |
| partition_id+%7B%0Aproject_id%3A+%22sample-project% … user-102%22% | Miller   | Apple   | true         |

*Sink Properties*

|   Name    |     Value      |
| --------- | -------------- |
| Project   | sample-project |
| Namespace | sample-ns      |
| Kind      | User           |
| Key Type  | URL-safe key   |

*Input Schema*

|     Name     |  Type   |
| ------------ | ------- |
| __key__      | STRING  |
| lastName     | STRING  |
| company      | STRING  |
| isContractor | BOOLEAN |

*Output in Cloud Datastore `Namespace: sample-ns`, `Kind: User`*

|    Name/ID    | lastName | company | isContractor |
| ------------- | -------- | ------- | ------------ |
| name=user-100 | Smith    | Netflix | false        |
| name=user-101 | Jones    | Google  | false        |
| name=user-102 | Miller   | Apple   | true         |
