# BigQuery Create Dataset

#### **Description**

Creates a BigQuery Dataset.

#### **Usage**

Dataset names must be unique per project. The dataset name contain up to 1,024 characters and can
contain letters (upper or lower case), numbers and underscore. Dataset names cannot contain special characters
such as '-', '&', '*', or '%'.

If dataset name starts with '_' (underscore) then the dataset would be hidden and not visible in
the view and dataset names are case sensitive (E.g. 'dataset' and 'Dataset' are two different
datasets that can co-exists in a project.

#### **Basic**

Following are properties used to configure this plugin

* **Name**

  Name of dataset to be created. May contain up to 1,024 characters and
  can contain letters (upper or lower case), numbers, and underscores

* **Description**

  Description to be associated with dataset.

* **Labels**

  GCP labels to be associated with the dataset.

* **Fail if exists**

  Configuration specifying if pipeline should fail when dataset already exists.

#### **Advanced**

If the plugin is run in GCP environment, the service account file path does not need to be
specified and can be set to 'auto-detect'. Credentials will be automatically read from the GCP environment.
A path to a service account key must be provided when not running in GCP. The service account
key can be found on the Dashboard in the Cloud Platform Console. Ensure that the account key has permission
to access resource.

* **Project Id**

  Google Cloud Project Id, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console. More information
can be found [here](https://cloud.google.com/resource-manager/docs/creating-managing-projects#identifying_projects)

* **Service Account File Path**

  Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running in GCP. When running on outside GCP,
the file must be present on every node were pipeline runs.