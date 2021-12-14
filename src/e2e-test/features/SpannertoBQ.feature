Feature: Spanner to BQ data transfer

  @Spanner
  Scenario: Verify data is getting transferred from Spanner to BigQuery
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner Connector
    When Target is BigQuery
    Then Connect Source as "Spanner" and sink as "BigQuery" to establish connection
    Then Open Spanner connector properties
    Then Enter the Spanner connector Properties
    Then Capture and validate output schema
    Then Validate Spanner connector properties
    Then Close the Spanner Properties
    Then Open BigQuery Properties
    Then Enter the BigQuery Sink properties for table "spannerBqTableName"
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for Spanner connector
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Get Count of no of records transferred to BigQuery in "spannerBqTableName"
    Then Open Logs
    Then Validate successMessage is displayed


  @Spanner
  Scenario: Verify data is getting transferred from Spanner to BigQuery with Import Query
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner Connector
    When Target is BigQuery
    Then Connect Source as "Spanner" and sink as "BigQuery" to establish connection
    Then Open Spanner connector properties
    Then Enter the Spanner connector Properties with Import Query "spannerQuery"
    Then Capture and validate output schema
    Then Validate Spanner connector properties
    Then Close the Spanner Properties
    Then Open BigQuery Properties
    Then Enter the BigQuery Sink properties for table "spannerBqTableName"
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for Spanner connector
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Get Count of no of records transferred to BigQuery in "spannerBqTableName"
    Then Open Logs
    Then Validate successMessage is displayed
