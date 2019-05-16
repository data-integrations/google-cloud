
#### **Description**

Publish events to Cloud Pub/Sub topic.

#### **Properties**

Following are properties used to configure this plugin

* **Reference Name**

  Name used to uniquely identify this sink for lineage, annotating metadata, etc.

* **Topic**

  Name of the Google Cloud PubSub topic to publish to.

* **Maximum Batch Count**

  Maximum number of messages to publish in a single batch.
Messages are published in batches to improve throughput. The default value is 100.

* **Maximum Batch Size**

  Maximum combined size of messages in kilobytes to publish in a single batch.
The default value is 1 KB.

* **Publish Delay Threshold**

  Maximum amount of time in milliseconds to wait before publishing a batch of messages.
The default value is 1 millisecond.

* **Retry Timeout**

  Maximum amount of time in seconds to retry publishing failures. The default value is 30 seconds.

* **Error Threshold**

  Maximum number of messages that failed to publish per partition before
the pipeline will be failed. The default value is 0.

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
