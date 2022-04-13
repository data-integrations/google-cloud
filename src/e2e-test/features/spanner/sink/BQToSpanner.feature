@Spanner_Sink @SPANNER_TEST
Feature: Spanner Sink - Verification of BigQuery to Spanner data transfer

  @BQ_SOURCE_TEST @SPANNER_SINK_TEST
  Scenario: To verify data is getting transferred successfully from BigQuery to Spanner existing database new table
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is Spanner
    Then Connect source as "BigQuery" and sink as "Spanner" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open Spanner sink properties
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Enter Spanner property InstanceId
    Then Enter Spanner sink property DatabaseName
    Then Enter Spanner sink property TableName
    Then Enter Spanner sink property primary key "spannerSinkPrimaryKeyBQ"
    Then Validate "Spanner" plugin properties
    Then Close the Spanner properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target spanner table with record counts of BigQuery table

  @BQ_SOURCE_TEST @SPANNER_SINK_NEWDB_TEST
  Scenario: To verify data is getting transferred successfully from BigQuery to Spanner new database
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is Spanner
    Then Connect source as "BigQuery" and sink as "Spanner" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open Spanner sink properties
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Enter Spanner property InstanceId
    Then Enter Spanner sink property DatabaseName
    Then Enter Spanner sink property TableName
    Then Enter Spanner sink property primary key "spannerSinkPrimaryKeyBQ"
    Then Enter Spanner sink property encryption key name "cmekSpanner" if cmek is enabled
    Then Validate "Spanner" plugin properties
    Then Close the Spanner properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target spanner table with record counts of BigQuery table
    Then Validate the cmek key "cmekSpanner" of target Spanner database if cmek is enabled
