
#### **Description**

Read from a Google Pub/Sub topic.

#### **Properties**

Following are properties used to configure this plugin

* **Reference Name**

  Name used to uniquely identify this sink for lineage, annotating metadata, etc.

* **Subscription**

  Name of the Google Cloud PubSub subscription to subscribe.
If the subscription needs to be created then the topic to which the subscription will belong must be provided.

* **Topic**

  Name of the Google Cloud PubSub topic to subscribe to. If a topic is provided and the given subscriber
does not exists it will be created. If a subscriber does not exists and is created only the messages arrived after
the creation of subscriber will be received.

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

