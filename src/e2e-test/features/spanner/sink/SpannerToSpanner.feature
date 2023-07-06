@Spanner_Sink @SPANNER_TEST
Feature: Spanner Sink - Verification of Spanner to Spanner data transfer

  @SPANNER_SINK_TEST
  Scenario: To verify data is getting transferred successfully from Spanner to Spanner with all supported datatype
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner
    When Sink is Spanner
    Then Connect source as "Spanner" and sink as "Spanner" to establish connection
    Then Open Spanner source properties
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter Spanner property InstanceId
    Then Enter Spanner source property DatabaseName
    Then Enter Spanner source property TableName
    Then Validate output schema with expectedSchema "spannerSourceSchema"
    Then Validate "Spanner" plugin properties
    Then Close the Spanner properties
    Then Open Spanner sink properties
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter Spanner property InstanceId
    Then Enter Spanner sink property DatabaseName
    Then Enter Spanner sink property TableName
    Then Enter Spanner sink property primary key "spannerSinkPrimaryKeySpanner"
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
    Then Validate records transferred to target spanner table with record counts of source spanner table
