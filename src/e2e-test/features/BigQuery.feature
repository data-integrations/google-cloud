Feature: BigQuery as source to GCS as sink connection

  @BigQuery
  Scenario:Transferring records from BigQuery to GCS
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Open Bigquery Properties
    Then Enter the BigQuery Properties for table "bqTableName" for source
    Then Capture output schema
    Then Close the BigQuery properties
    Then Enter the GCS properties
    Then Close the GCS Properties
    Then Connect Source as BigQuery and sink as GCS to establish connection
    Then Add pipeline name
    Then Click the preview
    Then Run and Preview Data
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Validate successMessage is displayed when pipeline is succeeded
    Then Validate the output record count

  @BigQuery
  Scenario:Validate BigQuery properties with filters
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is BigQuery
    Then Connect Source as BigQuery and sink as BigQuery to establish connection
    Then Open Bigquery Properties
    Then Enter the BigQuery Properties for filter on table "bqTableName" for source
    Then Capture output schema
    Then Close the BigQuery properties
    Then Open BigqueryTarget Properties
    Then Enter the BigQueryTarget Properties for table "tableDemo"
    Then Close the BigQueryTarget Properties
    Then Add pipeline name
    Then Click the preview
    Then Run and Preview Data
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Open the Logs and capture raw logs
    Then Validate successMessage is displayed when pipeline is succeeded
    Then Get Count of no of records transferred to BigQuery in "tableDemo"
    Then Validate record transferred from table "tableDemo" on the basis of filter "bqFilter" is equal to the total no of records
    Then Delete the table "tableDemo"

  @BigQuery
  Scenario:Validate that pipeline run preview gets failed when incorrect filter values are provided
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is BigQuery
    Then Connect Source as BigQuery and sink as BigQuery to establish connection
    Then Open Bigquery Properties
    Then Enter the BigQuery Properties with incorrect filter "bqinvalidFilter" values in the fields on table "bqTableName" for source
    Then Close the BigQuery properties
    Then Open BigqueryTarget Properties
    Then Enter the BigQueryTarget Properties for table "tableDemo"
    Then Close the BigQueryTarget Properties
    Then Click the preview
    Then Verify the preview of pipeline is "failed"

  @BigQuery
  Scenario: Verify records are getting transferred with respect to partitioned date
    Given Delete the table "tableDemo"
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is BigQuery
    Then Connect Source as BigQuery and sink as BigQuery to establish connection
    Then Open Bigquery Properties
    Then Enter the partitionStartDate "bqPartitionstartdate" and  partitionEndDate "bqPartitionenddate"
    Then Enter the BigQuery Properties for table "bqPartionedTable" for source
    Then Capture output schema
    Then Close the BigQuery properties
    Then Enter the BigQueryTarget Properties for table "tableDemo"
    Then Close the BigQueryTarget Properties
    Then Add pipeline name
    Then Click the preview
    Then Run and Preview Data
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Get Count of no of records transferred to BigQuery in "tableDemo"
    Then Validate Partition date of table- "bqPartionedTable" is equal to output partitioned table "tableDemo"


  @BigQuery
  Scenario: Verify records are not getting transferred for future partitioned date
    Given Delete the table "tableDemo"
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is BigQuery
    Then Connect Source as BigQuery and sink as BigQuery to establish connection
    Then Open Bigquery Properties
    Then Enter the partitionStartDate "bqFuturePartionStartDate" and  partitionEndDate "bqFuturePartionEndDate"
    Then Enter the BigQuery Properties for table "bqPartionedTable" for source
    Then Close the BigQuery properties
    Then Enter the BigQueryTarget Properties for table "tableDemo"
    Then Close the BigQueryTarget Properties
    Then Save and Deploy BQ Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the records are not created in output table "tableDemo"
    Then Validate partitioning is not done on the output table "tableDemo"

  @BigQuery
  Scenario:Verify BigQuery properties validation fails when Reference Name is not provided
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    Then Open Bigquery Properties
    Then Enter the BigQuery Properties for table "bqTableName" without Reference Name
    Then Verify Reference Field throws error

  @BigQuery
  Scenario:Verify BigQuery properties validation fails when Dataset value is not provided
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    Then Open Bigquery Properties
    Then Enter the BigQuery Properties for table "bqTableName" without dataset field
    Then Verify Dataset Field throws error

  @BigQuery
  Scenario:Verify BigQuery properties validation fails when table name is not provided
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    Then Open Bigquery Properties
    Then Enter the BigQuery Properties for table "bqTableName" without table name field
    Then Verify Table Field throws error

  @BigQuery
  Scenario:Verify Bigquery plugin Sink mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Target is BigQuery
    Then Open Bigquery Properties
    Then Verify mandatory Fields

