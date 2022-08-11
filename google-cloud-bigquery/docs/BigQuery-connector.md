# Google BigQuery Connection

Description
-----------
Use this connection to access data in Google BigQuery.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project. This is where the BigQuery job will be run
to query the data. It can be found on the Dashboard in the Google Cloud Platform Console. Make sure the service account
specified below has the permission to run BigQuery jobs in this project by granting the service account
the `BigQuery Job User` role.

**Dataset Project ID**: Google Cloud Project ID of the project where your dataset is. It can be found on the Dashboard
in the Google Cloud Platform Console. Make sure the service account specified below has the permission to view data in
this project by granting the service account the `BigQuery Data Viewer` role.

**Service Account**: When running on Google Cloud Platform, the service account key does not need to be provided, as
it can automatically be read from the environment. In other environments, the service account key must be provided.

* **File Path**: Path on the local file system of the service account key used for authorization. Can be set to '
  auto-detect' when running on a Dataproc cluster. When running on other clusters, the file must be present on every
  node in the cluster.

* **JSON**: Contents of the service account JSON file.

**Show Hidden Datasets:** Whether to show hidden datasets.

Path of the connection
----------------------
To browse, get a sample from, or get the specification for this connection through 
[Pipeline Microservices](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/975929350/Pipeline+Microservices), the `path`
property is required in the request body. It can be in the following form :

1. `/{dataset}/{table}`
   This path indicates a table. A table is the only one that can be sampled. Browse on this path to return the specified table.

2. `/{dataset}`
   This path indicates a dataset. A dataset cannot be sampled. Browse on this path to get all the tables under this dataset.

3. `/`
   This path indicates the root. A root cannot be sampled. Browse on this path to get all the datasets visible through this connection.

Trouble Shooting
----------------

**missing permission to run BigQuery jobs**

If your pipeline failed with the following error in the log:
```
POST https://bigquery.googleapis.com/bigquery/v2/projects/xxxx/jobs
{
"code" : 403,
"errors" : [ {
"domain" : "global",
"message" : "Access Denied: Project xxxx: User does not have bigquery.jobs.create permission in project xxxx",
"reason" : "accessDenied"
} ],
"message" : "Access Denied: Project xxxx: User does not have bigquery.jobs.create permission in project xxxx.",
"status" : "PERMISSION_DENIED"
}
``` 
`xxxx` is the `Project ID` you specified in this plugin. This means the specified service account doesn't have the
permission to run BigQuery jobs. You must grant "BigQuery Job User" role on the project identified by the `Project ID`
you specified in this plugin to the service account. If you think you already granted the role, check if you granted the
role on the wrong project (for example the one identified by the `Dataset Project ID`).

**missing permission to read the BigQuery dataset**
If your pipeline failed with the following error in the log:
```
com.google.api.client.googleapis.json.GoogleJsonResponseException: 403 Forbidden
GET https://www.googleapis.com/bigquery/v2/projects/xxxx/datasets/mysql_bq_perm?prettyPrint=false
{
"code" : 403,
"errors" : [ {
"domain" : "global",
"message" : "Access Denied: Dataset xxxx:mysql_bq_perm: Permission bigquery.datasets.get denied on dataset xxxx:mysql_bq_perm (or it may not exist).",
"reason" : "accessDenied"
} ],
"message" : "Access Denied: Dataset xxxx:mysql_bq_perm: Permission bigquery.datasets.get denied on dataset xxxx:mysql_bq_perm (or it may not exist).",
"status" : "PERMISSION_DENIED"
}
```
`xxxx` is the `Dataset Project ID` you specified in this plugin. The service account you specified in this plugin doesn't
have the permission to read the dataset you specified in this plugin. You must grant "BigQuery Data Viewer" role on the
project identified by the `Dataset Project ID` you specified in this plugin to the service account. If you think you
already granted the role, check if you granted the role on the wrong project (for example the one identified by the `Project ID`).

