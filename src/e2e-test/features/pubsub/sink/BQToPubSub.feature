@PubSub_Sink @PUBSUB_SINK_TEST
Feature: PubSub-Sink - Verification of BigQuery to PubSub successful data transfer

  @CMEK @BQ_SOURCE_TEST
  Scenario: To verify data is getting transferred from BigQuery to PubSub successfully
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is PubSub
    Then Connect source as "BigQuery" and sink as "GooglePublisher" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Enter PubSub property reference name
    Then Enter PubSub topic name
    Then Select PubSub property format "csv"
    Then Enter PubSub property encryption key name "cmekPubSub" if cmek is enabled
    Then Enter PubSub property MaximumBatchCount "pubSubMaximumBatchCount"
    Then Enter PubSub property MaximumBatchSize "pubSubMaximumBatchSize"
    Then Enter PubSub property PublishDelayThreshold "pubSubPublishDelayThreshold"
    Then Enter PubSub property RetryTimeOut "pubSubRetryTimeOut"
    Then Enter PubSub property ErrorThreshold "pubSubErrorThreshold"
    Then Validate "PubSub" plugin properties
    Then Close the PubSub properties
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
