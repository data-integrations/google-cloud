@GCS_Sink
Feature: GCS sink - Verification of GCS Sink plugin

  @CMEK @GCS_SINK_TEST @BQ_SOURCE_TEST
  Scenario: To verify data is getting transferred successfully from BigQuery to GCS
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Open BigQuery source properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS sink property path
    Then Select GCS property format "csv"
    Then Enter GCS property encryption key name "cmekGCS" if cmek is enabled
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
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
    Then Validate the cmek key "cmekGCS" of target GCS bucket if cmek is enabled
