@BigQuery_Sink
Feature: BigQuery sink - Verification of GCS to BigQuery successful data transfer

  @CMEK @GCS_CSV_TEST @BQ_SINK_TEST @BigQuery_Sink_Required
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
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Then Validate the cmek key "cmekBQ" of target BigQuery table if cmek is enabled

  @GCS_CSV_RANGE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from GCS to BigQuery with partition type INTEGER
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvRangeFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsCsvRangeFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery sink property table name
    Then Toggle BigQuery sink property truncateTable to true
    Then Toggle BigQuery sink property updateTableSchema to true
    Then Select BigQuery sink property partitioning type as "INTEGER"
    Then Enter BigQuery sink property range start "bqRangeStart"
    Then Enter BigQuery sink property range end "bqRangeEnd"
    Then Enter BigQuery sink property range interval "bqRangeInterval"
    Then Enter BigQuery sink property partition field "bqPartitionField"
    Then Toggle BigQuery sink property require partition filter to true
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
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
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify the partition table is created with partitioned on field "bqPartitionField"

  @GCS_CSV_RANGE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from GCS to BigQuery with partition type NONE
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvRangeFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsCsvRangeFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery sink property table name
    Then Toggle BigQuery sink property truncateTable to true
    Then Toggle BigQuery sink property updateTableSchema to true
    Then Select BigQuery sink property partitioning type as "NONE"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
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
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table

  @GCS_CSV_RANGE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from GCS to BigQuery with GCS upload request chunk size
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsCsvRangeFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsCsvRangeFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery sink property table name
    Then Enter BigQuery sink property GCS upload request chunk size "bqChunkSize"
    Then Toggle BigQuery sink property truncateTable to true
    Then Toggle BigQuery sink property updateTableSchema to true
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
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
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
