@BigQuery_Sink
Feature: BigQuery sink - Verification of GCS to BigQuery successful data transfer

  @CMEK @GCS_CSV_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from GCS to BigQuery
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Open GCS source properties
    Then Enter the GCS source mandatory properties
    Then Override Service account details if set in environment variables
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery sink property table name
    Then Enter BiqQuery property encryption key name "cmekBQ" if cmek is enabled
    Then Toggle BigQuery sink property truncateTable to true
    Then Toggle BigQuery sink property updateTableSchema to true
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Then Validate the cmek key "cmekBQ" of target BigQuery table if cmek is enabled
