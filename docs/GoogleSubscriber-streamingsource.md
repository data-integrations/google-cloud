### Google Subscriber Streaming source

Streaming source plugin to read from Google PubSub. Reads the events from pubsub and passes in the message id, message timestamp and the message as fields
of output schema of the source. The output schema of the source is fixed and cannot be changed.

## Properties

**Project ID:** Project ID of the Google PubSub project

**Subscriber ID:** Subscriber ID to read from the PubSub topic

**Service File path:** Service File Path that contains the private key. Note the service file should be distributed on all the nodes of the cluster.

