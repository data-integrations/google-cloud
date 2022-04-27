@BigQuery_Source
Feature: BigQuery source - Verification of BigQuery to BigQuery successful data transfer

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with filter
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is BigQuery
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Enter BigQuery source property filter "bqFilter"
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "BigQuery" and sink as "BigQuery" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on preview data for BigQuery sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Then Validate records transferred to target table is equal to number of records from source table with filter "bqFilter"

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario:Validate that pipeline run preview gets failed when incorrect filter values are provided
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is BigQuery
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Enter BigQuery source property filter "bqInvalidFilter"
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "BigQuery" and sink as "BigQuery" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "failed"

  @BQ_PARTITIONED_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Verify records are getting transferred with respect to partitioned date
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is BigQuery
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Enter BigQuery source properties partitionStartDate and partitionEndDate
    Then Validate output schema with expectedSchema "bqPartitionSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "BigQuery" and sink as "BigQuery" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on preview data for BigQuery sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Then Validate partition date in output partitioned table

  @BQ_PARTITIONED_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Verify records are not getting transferred for future partitioned date
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is BigQuery
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Enter BigQuery source properties partitionStartDate "bqFuturePartitionStartDate" and partitionEndDate "bqFuturePartitionEndDate"
    Then Validate output schema with expectedSchema "bqPartitionSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "BigQuery" and sink as "BigQuery" to establish connection
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the records are not created in output table
    Then Validate partitioning is not done on the output table

  @BQ_SOURCE_DATATYPE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with default time partitioning type
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is BigQuery
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Validate output schema with expectedSchema "bqSourceSchemaDatatype"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery sink property table name
    Then Toggle BigQuery sink property truncateTable to true
    Then Toggle BigQuery sink property updateTableSchema to true
    Then Enter BigQuery sink property partition field "bqPartitionFieldTime"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "BigQuery" and sink as "BigQuery" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify the partition table is created with partitioned on field "bqPartitionFieldTime"

  @BQ_SOURCE_DATATYPE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with all the datatypes
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is BigQuery
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Validate output schema with expectedSchema "bqSourceSchemaDatatype"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery sink property table name
    Then Toggle BigQuery sink property truncateTable to true
    Then Toggle BigQuery sink property updateTableSchema to true
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "BigQuery" and sink as "BigQuery" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target table is equal to number of records from source table
