@BQMT_Sink
Feature: BQMT-Sink - Verify BigQuery to BQMT sink plugin data transfer scenarios

  @CMEK @BQ_SOURCE_BQMT_TEST @BQ_SINK_TEST_CSV_BQMT
  Scenario: Verify data is getting transferred from BigQuery to BQMT sink with all datatypes
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is BiqQueryMultiTable
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Validate output schema with expectedSchema "bqmtSourceSchemaDatatype"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Toggle BiqQueryMultiTable sink property truncateTable to true
    Then Select BiqQueryMultiTable sink property allow flexible schema to true
    Then Select BiqQueryMultiTable sink property update table schema as "true"
    Then Enter BiqQueryMultiTable sink property encryption key name "cmekBQ" if cmek is enabled
    Then Validate "BigQueryMultiTable" plugin properties
    Then Close the BiqQueryMultiTable properties
    Then Connect source as "BigQuery" and sink as "BigQueryMultiTable" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Validate the cmek key "cmekBQ" of target BQMT tables "bqmtCsvTargetTables" if cmek is enabled
    Then Validate records transferred to target BQMT tables "bqmtCsvTargetTables" is equal to number of records from source BQ table
