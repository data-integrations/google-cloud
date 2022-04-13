@Spanner_Source @SPANNER_TEST
Feature: Spanner Source - Verification of Spanner to BigQuery successful data transfer

  @BQ_SINK_TEST
  Scenario: Verify data is getting transferred from Spanner to BigQuery successfully
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner
    When Sink is BigQuery
    Then Connect source as "Spanner" and sink as "BigQuery" to establish connection
    Then Open Spanner source properties
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Enter Spanner property InstanceId
    Then Enter Spanner source property DatabaseName
    Then Enter Spanner source property TableName
    Then Validate output schema with expectedSchema "spannerSourceSchema"
    Then Validate "Spanner" plugin properties
    Then Close the Spanner properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
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
    Then Validate records transferred to target BigQuery table with record counts of spanner table

  @BQ_SINK_TEST
  Scenario: Verify data is getting transferred from Spanner to BigQuery successfully with Import Query
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner
    When Sink is BigQuery
    Then Connect source as "Spanner" and sink as "BigQuery" to establish connection
    Then Open Spanner source properties
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Enter Spanner property InstanceId
    Then Enter Spanner source property DatabaseName
    Then Enter Spanner source property TableName
    Then Enter the Spanner source property Import Query "spannerQuery"
    Then Validate output schema with expectedSchema "spannerSourceSchema"
    Then Validate "Spanner" plugin properties
    Then Close the Spanner properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
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
    Then Validate records transferred to target BigQuery table with record counts of spanner Import Query "spannerCountQuery"
