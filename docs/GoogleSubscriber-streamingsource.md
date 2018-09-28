# Google Cloud PubSub Streaming Source

Description
-----------
This sources reads from a Google Cloud Pub/Sub subscription in realtime.
Cloud Pub/Sub brings the scalability, flexibility, and reliability of enterprise message-oriented
middleware to the cloud. By providing many-to-many, asynchronous messaging that decouples senders and receivers,
it allows for secure and highly available communication between independently written applications.

Credentials
-----------
If the plugin is run on a Google Cloud Dataproc cluster, the service account key does not need to be
provided and can be set to 'auto-detect'.
Credentials will be automatically read from the cluster environment.

If the plugin is not run on a Dataproc cluster, the path to a service account key must be provided.
The service account key can be found on the Dashboard in the Cloud Platform Console.
Make sure the account key has permission to access Google Cloud Pub/Sub.
The service account key file needs to be available on every node in your cluster and
must be readable by all users running the job.

Properties
----------
**Reference Name:** Name used to uniquely identify this sink for lineage, annotating metadata, etc.

**Project ID**: Google Cloud Project ID, which uniquely identifies a project.
It can be found on the Dashboard in the Google Cloud Platform Console.

**Subscription**: Name of the Google Cloud PubSub subscription to subscribe.
If the subscription needs to be created then the topic to which the subscription will belong must be provided.

**Topic**: Name of the Google Cloud PubSub topic to subscribe to. If a topic is provided and the given subscriber
does not exists it will be created. If a subscriber does not exists and is created only the messages arrived after
the creation of subscriber will be received.

**Service Account File Path**: Path on the local file system of the service account key used for
authorization. Can be set to 'auto-detect' when running on a Dataproc cluster.
When running on other clusters, the file must be present on every node in the cluster.
