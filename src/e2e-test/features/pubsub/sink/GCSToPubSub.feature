@PubSub_Sink @PUBSUB_SINK_TEST
Feature: PubSub-Sink - Verification of GCS to PubSub successful data transfer

  @CMEK @GCS_CSV_TEST
  Scenario: To verify data is getting transferred from GCS to PubSub successfully
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is PubSub
    Then Open GCS source properties
    Then Enter the GCS source mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Enter PubSub sink property encryption key name "cmekPubSub" if cmek is enabled
    Then Enter PubSub sink property MaximumBatchCount "pubSubMaximumBatchCount"
    Then Enter PubSub sink property MaximumBatchSize "pubSubMaximumBatchSize"
    Then Enter PubSub sink property PublishDelayThreshold "pubSubPublishDelayThreshold"
    Then Enter PubSub sink property RetryTimeOut "pubSubRetryTimeOut"
    Then Enter PubSub sink property ErrorThreshold "pubSubErrorThreshold"
    Then Select PubSub property format "csv"
    Then Validate "PubSub" plugin properties
    Then Close the PubSub properties
    Then Connect source as "GCS" and sink as "GooglePublisher" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Validate the cmek key "cmekPubSub" of target PubSub topic if cmek is enabled

  @GCS_CSV_TEST
  Scenario Outline: Verify data is getting transferred from GCS to PubSub for different file formats.
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is PubSub
    Then Open GCS source properties
    Then Enter the GCS source mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Enter PubSub sink property MaximumBatchCount "pubSubMaximumBatchCount"
    Then Enter PubSub sink property MaximumBatchSize "pubSubMaximumBatchSize"
    Then Enter PubSub sink property PublishDelayThreshold "pubSubPublishDelayThreshold"
    Then Enter PubSub sink property RetryTimeOut "pubSubRetryTimeOut"
    Then Enter PubSub sink property ErrorThreshold "pubSubErrorThreshold"
    Then Select PubSub property format "<FileFormat>"
    Then Validate "PubSub" plugin properties
    Then Close the PubSub properties
    Then Connect source as "GCS" and sink as "GooglePublisher" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Examples:
      |FileFormat     |
      | text          |
      | avro          |
      | blob          |
      | tsv           |
      | json          |
      | parquet       |
