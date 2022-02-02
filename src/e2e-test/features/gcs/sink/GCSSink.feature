@GCS_Sink
Feature: Verification of GCS Sink plugin

  @CMEK @GCS_TARGET_TEST @BQ_SOURCE_TEST
  Scenario: To verify data is getting transferred successfully from BigQuery to GCS
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GCS bucket
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery Properties
    Then Open GCS Sink Properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS sink property path
    Then Select GCS property format "csv"
    Then Enter GCS property encryption key name if cmek is enabled
    Then Validate "GCS" plugin properties
    Then Close the GCS Properties
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
    Then Validate the cmek key of target GCS bucket if cmek is enabled
