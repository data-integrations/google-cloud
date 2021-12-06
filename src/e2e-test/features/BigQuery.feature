Feature: Validate BigQuery Plugin

  @BigQuery
  Scenario:Validate successful records transfer from BigQuery to GCS
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Open BigQuery Properties
    Then Enter the BigQuery properties for table "bqTableName"
    Then Capture output schema
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Enter the GCS properties
    Then Close the GCS Properties
    Then Connect Source as "BigQuery" and sink as "GCS" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for BigQuery
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Validate successMessage is displayed when pipeline is succeeded
    Then Validate the output record count

  @BigQuery
  Scenario:Validate successful records transfer from BigQuery to BigQuery with filter
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is BigQuery
    Then Open BigQuery Properties
    Then Enter the BigQuery Properties for table "bqTableName" with filter "bqFilter"
    Then Capture output schema
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery Target Properties for table "bqTableDemo"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "BigQuery" and sink as "BigQuery2" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for BigQuery
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the output record count
    Then Open the Logs and capture raw logs
    Then Validate successMessage is displayed when pipeline is succeeded
    Then Get Count of no of records transferred to BigQuery in "bqTableDemo"
    Then Validate record transferred from table "bqTableDemo" on the basis of filter "bqFilter" is equal to the total no of records
    Then Delete the table "bqTableDemo"

  @BigQuery
  Scenario:Validate that pipeline run preview gets failed when incorrect filter values are provided
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is BigQuery
    Then Open BigQuery Properties
    Then Enter the BigQuery Properties for table "bqTableName" with filter "bqInvalidFilter"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery Target Properties for table "bqTableDemo"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "BigQuery" and sink as "BigQuery2" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "failed"

  @BigQuery
  Scenario: Verify records are getting transferred with respect to partitioned date
    Given Delete the table "bqTableDemo"
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is BigQuery
    Then Open BigQuery Properties
    Then Enter the BigQuery properties for table "bqPartitionedTable" with partitionStartDate "bqPartitionStartDate" and partitionEndDate "bqPartitionEndDate"
    Then Capture output schema
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery Target Properties for table "bqTableDemo"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "BigQuery" and sink as "BigQuery2" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for BigQuery
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Get Count of no of records transferred to BigQuery in "bqTableDemo"
    Then Validate partition date in output partitioned table "bqTableDemo"

  @BigQuery
  Scenario: Verify records are not getting transferred for future partitioned date
    Given Delete the table "bqTableDemo"
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is BigQuery
    Then Open BigQuery Properties
    Then Enter the BigQuery properties for table "bqPartitionedTable" with partitionStartDate "bqFuturePartitionStartDate" and partitionEndDate "bqFuturePartitionEndDate"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery Target Properties for table "bqTableDemo"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "BigQuery" and sink as "BigQuery2" to establish connection
    Then Save and Deploy BQ Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate the records are not created in output table "bqTableDemo"
    Then Validate partitioning is not done on the output table "bqTableDemo"

  @BigQuery
  Scenario Outline:Verify BigQuery Source properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    Then Open BigQuery Properties
    Then Enter the BigQuery Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property |
      | referenceName  |
      | dataset  |
      | table  |

  @BigQuery
  Scenario Outline:Verify BigQuery Source properties validation errors for incorrect values
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    Then Open BigQuery Properties
    Then Enter the BigQuery Properties with incorrect property "<property>" value "<value>"
    Then Validate incorrect property error for table "<property>" value "<value>"
    Examples:
      | property        | value                       |
      | dataset         | bqIncorrectDataset          |
      | table           | bqIncorrectTableName        |
      | datasetProject  | bqIncorrectDatasetProjectId |

  @BigQuery
  Scenario Outline:Verify BigQuery Source properties validation errors for incorrect format of projectIds
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    Then Open BigQuery Properties
    Then Enter the BigQuery Properties with incorrect projectId format "<field>" value "<value>"
    Then Verify plugin properties validation fails with error
    Examples:
      | field           | value                       |
      | project         | bqIncorrectFormatProjectId        |
      | datasetProject  | bqIncorrectFormatDatasetProjectId |

  @BigQuery
  Scenario Outline:Verify BigQuery Sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Target is BigQuery
    Then Open BigQuery Properties
    Then Enter the BigQuery Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property |
      | referenceName  |
      | dataset  |
      | table  |
