@BigQuery_Source
Feature: BigQuery source - Verification of BigQuery to Multiple sinks successful data transfer

  @CMEK @BQ_SOURCE_TEST @GCS_SINK_TEST @BQ_SINK_TEST @PUBSUB_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to multiple sinks (GCS, BigQuery and PubSub)
    Given Open Datafusion Project to configure pipeline
    # START - Workaround till https://cdap.atlassian.net/browse/CDAP-18862 gets fixed. Remove once issue is fixed.
    And Wait for page to render properly
    # END
    When Source is BigQuery
    When Sink is GCS
    Then Connect source as "BigQuery" having title "BigQuery" and sink1 as "GCS" having title "GCS" to establish connection
    When Sink is BigQuery
    Then Connect source as "BigQuery" having title "BigQuery" and sink2 as "BigQuery" having title "BigQuery2" to establish connection
    When Sink is PubSub
    Then Connect source as "BigQuery" having title "BigQuery" and sink3 as "GooglePublisher" having title "Pub/Sub" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter the GCS sink mandatory properties
    Then Enter GCS property encryption key name "cmekGCS" if cmek is enabled
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Enter BiqQuery property encryption key name "cmekBQ" if cmek is enabled
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open the PubSub sink properties
    Then Enter the PubSub sink mandatory properties
    Then Enter PubSub sink property encryption key name "cmekPubSub" if cmek is enabled
    Then Validate "PubSub" plugin properties
    Then Close the PubSub properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
    Then Get count of no of records transferred to target BigQuery Table
    Then Validate the cmek key "cmekGCS" of target GCS bucket if cmek is enabled
    Then Validate the cmek key "cmekBQ" of target BigQuery table if cmek is enabled
    Then Validate the cmek key "cmekPubSub" of target PubSub topic if cmek is enabled
