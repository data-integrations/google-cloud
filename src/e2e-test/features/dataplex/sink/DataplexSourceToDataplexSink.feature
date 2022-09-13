@Dataplex_Sink
Feature: Dataplex sink - Verification of BQ source to dataplex sink with update metadata enabled

  @Dataplex_SINK_TEST @GCS_SINK_TEST @BQ_SOURCE_TEST
  Scenario:Validate successful records transfer from BigQuery to Dataplex with update metadata enabled
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is Dataplex
    Then Connect source as "BigQuery" and sink as "Dataplex" to establish connection
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
    Then Open Dataplex sink properties
    Then Override Service account details if set in environment variables
    Then Enter the Dataplex mandatory properties
    Then Enter the Dataplex sink mandatory properties
    Then Enable Metadata Update
    Then Validate "Dataplex" plugin properties
    Then Close the Dataplex properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Given Open Datafusion Project to configure pipeline
    When Source is Dataplex
    When Sink is Dataplex
    Then Connect source as "Dataplex" and sink as "Dataplex" to establish connection
    Then Open Dataplex source properties
    Then Enter the Dataplex mandatory properties
    Then Enter the Dataplex source mandatory properties
    Then Override Service account details if set in environment variables
    Then Validate output schema with expectedSchema "dataplexSourceSchema"
    Then Validate "Dataplex" plugin properties
    Then Close the Dataplex properties
    Then Open Dataplex sink properties
    Then Override Service account details if set in environment variables
    Then Enter the Dataplex mandatory properties
    Then Enter the Dataplex sink mandatory properties
    Then Enable Metadata Update
    Then Remove "ts" column from output schema
    Then Validate "Dataplex" plugin properties
    Then Close the Dataplex properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
