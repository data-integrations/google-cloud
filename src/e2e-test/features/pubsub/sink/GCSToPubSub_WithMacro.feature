@PubSub_Sink @PUBSUB_SINK_TEST
Feature: PubSub-Sink - Verification of GCS to PubSub successful data transfer with macro arguments

  @CMEK @GCS_CSV_TEST
  Scenario:Validate successful records transfer from GCS to PubSub with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is PubSub
    Then Open GCS source properties
    Then Enter GCS property reference name
    Then Enter GCS property "projectId" as macro argument "gcsProjectId"
    Then Enter GCS property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter GCS property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter GCS property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter GCS property "path" as macro argument "gcsSourcePath"
    Then Enter GCS property "format" as macro argument "gcsFormat"
    Then Enter GCS source property "skipHeader" as macro argument "gcsSkipHeader"
    Then Enter GCS source property output schema "outputSchema" as macro argument "gcsOutputSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the PubSub sink properties
    Then Enter PubSub property reference name
    Then Enter PubSub property "projectId" as macro argument "PubSubProjectId"
    Then Enter PubSub property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter PubSub property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter PubSub property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter PubSub property "topic" as macro argument "PubSubSinkTopic"
    Then Enter PubSub property "format" as macro argument "PubSubFormat"
    Then Enter PubSub sink cmek property "encryptionKeyName" as macro argument "cmekPubSub" if cmek is enabled
    Then Validate "GooglePublisher" plugin properties
    Then Close the PubSub properties
    Then Connect source as "GCS" and sink as "GooglePublisher" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "PubSubProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value for PubSub sink property topic key "PubSubSinkTopic"
    Then Enter runtime argument value "csvFormat" for key "PubSubFormat"
    Then Enter runtime argument value "cmekPubSub" for PubSub Sink cmek property key "cmekPubSub" if PubSub Sink cmek is enabled
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value "gcsCsvFile" for GCS source property path key "gcsSourcePath"
    Then Enter runtime argument value "gcsSkipHeaderTrue" for key "gcsSkipHeader"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "gcsCSVFileOutputSchema" for key "gcsOutputSchema"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "PubSubProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value for PubSub sink property topic key "PubSubSinkTopic"
    Then Enter runtime argument value "csvFormat" for key "PubSubFormat"
    Then Enter runtime argument value "cmekPubSub" for PubSub Sink cmek property key "cmekPubSub" if PubSub Sink cmek is enabled
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value "gcsCsvFile" for GCS source property path key "gcsSourcePath"
    Then Enter runtime argument value "gcsSkipHeaderTrue" for key "gcsSkipHeader"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "gcsCSVFileOutputSchema" for key "gcsOutputSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"

  @GCS_CSV_TEST
  Scenario:Validate successful records transfer from GCS to PubSub using advance fields with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is PubSub
    Then Open GCS source properties
    Then Enter GCS property reference name
    Then Enter GCS property "projectId" as macro argument "gcsProjectId"
    Then Enter GCS property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter GCS property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter GCS property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter GCS property "path" as macro argument "gcsSourcePath"
    Then Enter GCS property "format" as macro argument "gcsFormat"
    Then Enter GCS source property "skipHeader" as macro argument "gcsSkipHeader"
    Then Enter GCS source property output schema "outputSchema" as macro argument "gcsOutputSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the PubSub sink properties
    Then Enter PubSub property reference name
    Then Enter PubSub property "projectId" as macro argument "PubSubProjectId"
    Then Enter PubSub property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter PubSub property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter PubSub property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter PubSub property "topic" as macro argument "PubSubSinkTopic"
    Then Enter PubSub property "format" as macro argument "PubSubFormat"
    Then Enter PubSub property "maximumBatchCount" as macro argument "maximumBatchCount"
    Then Enter PubSub property "maximumBatchSize" as macro argument "maximumBatchSize"
    Then Enter PubSub property "publishDelayThreshold" as macro argument "publishDelayThreshold"
    Then Enter PubSub property "retryTimeout" as macro argument "retryTimeout"
    Then Enter PubSub property "errorThreshold" as macro argument "errorThreshold"
    Then Validate "GooglePublisher" plugin properties
    Then Close the PubSub properties
    Then Connect source as "GCS" and sink as "GooglePublisher" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "PubSubProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value for PubSub sink property topic key "PubSubSinkTopic"
    Then Enter runtime argument value "csvFormat" for key "PubSubFormat"
    Then Enter runtime argument value "pubSubMaximumBatchCount" for key "maximumBatchCount"
    Then Enter runtime argument value "pubSubMaximumBatchSize" for key "maximumBatchSize"
    Then Enter runtime argument value "pubSubPublishDelayThreshold" for key "publishDelayThreshold"
    Then Enter runtime argument value "pubSubRetryTimeOut" for key "retryTimeout"
    Then Enter runtime argument value "pubSubErrorThreshold" for key "errorThreshold"
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value "gcsCsvFile" for GCS source property path key "gcsSourcePath"
    Then Enter runtime argument value "gcsSkipHeaderTrue" for key "gcsSkipHeader"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "gcsCSVFileOutputSchema" for key "gcsOutputSchema"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "PubSubProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value for PubSub sink property topic key "PubSubSinkTopic"
    Then Enter runtime argument value "csvFormat" for key "PubSubFormat"
    Then Enter runtime argument value "pubSubMaximumBatchCount" for key "maximumBatchCount"
    Then Enter runtime argument value "pubSubMaximumBatchSize" for key "maximumBatchSize"
    Then Enter runtime argument value "pubSubPublishDelayThreshold" for key "publishDelayThreshold"
    Then Enter runtime argument value "pubSubRetryTimeOut" for key "retryTimeout"
    Then Enter runtime argument value "pubSubErrorThreshold" for key "errorThreshold"
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value "gcsCsvFile" for GCS source property path key "gcsSourcePath"
    Then Enter runtime argument value "gcsSkipHeaderTrue" for key "gcsSkipHeader"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "gcsCSVFileOutputSchema" for key "gcsOutputSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
