@BigQuery_Source
Feature: BigQuery source - Verification of BigQuery to GCS successful data transfer

  @CMEK @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to GCS
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Override Service account details if set in environment variables
    Then Enter BiqQuery property encryption key name "cmekBQ" if cmek is enabled
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS sink mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on preview data for GCS sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
