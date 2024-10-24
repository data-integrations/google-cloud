@Spanner_Sink @SPANNER_TEST
Feature: Spanner Sink - Verification of GCS to Spanner data transfer

  @GCS_CSV_TEST @SPANNER_SINK_TEST
  Scenario: To verify data is getting transferred successfully from GCS to Spanner existing database new table
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is Spanner
    Then Connect source as "GCS" and sink as "Spanner" to establish connection
    Then Open GCS source properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS source mandatory properties
    Then Validate "GCS" plugin propserties
    Then Close the GCS properties
    Then Open Spanner sink properties
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter Spanner property InstanceId
    Then Enter Spanner sink property DatabaseName
    Then Enter Spanner sink property TableName
    Then Enter Spanner sink property primary key "spannerSinkPrimaryKeyGCS"
    Then Validate "Spanner" plugin properties
    Then Close the Spanner properties
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

  @GCS_CSV_TEST @SPANNER_SINK_NEWDB_TEST
  Scenario: To verify data is getting transferred successfully from GCS to Spanner new database
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is Spanner
    Then Connect source as "GCS" and sink as "Spanner" to establish connection
    Then Open GCS source properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS source mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open Spanner sink properties
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter Spanner property InstanceId
    Then Enter Spanner sink property DatabaseName
    Then Enter Spanner sink property TableName
    Then Enter Spanner sink property primary key "spannerSinkPrimaryKeyGCS"
    Then Enter Spanner sink property encryption key name "cmekSpanner" if cmek is enabled
    Then Validate "Spanner" plugin properties
    Then Close the Spanner properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipseline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count is equal to IN record count
    Then Validate the cmek key "cmekSpanner" of target Spanner database if cmek is enabled
