@Spanner_Sources
Feature: Spanner source - Verification of Additional Spanner to Spanner successful data transfer without using connections

  @SPANNER_TEST @EXISTING_SPANNER_SINK
  Scenario: To verify data is getting transferred from no schema Spanner source to Spanner sink having existing table
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Spanner" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Spanner" from the plugins list as: "Sink"
    Then Connect plugins: "Spanner" and "Spanner2" to establish connection
    Then Navigate to the properties page of plugin: "Spanner"
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "instanceId" with value: "spannerInstance"
    Then Enter input plugin property: "databaseName" with value: "spannerDatabase"
    Then Enter input plugin property: "tableName" with value: "spannerSourceTable"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Spanner2"
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "instanceId" with value: "spannerInstance"
    Then Enter input plugin property: "databaseName" with value: "spannerDatabase"
    Then Enter input plugin property: "tableName" with value: "spannerExistingTargetTable"
    Then Enter Spanner sink property primary key "spannerSinkPrimaryKeySpanner"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
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
    Then Validate records transferred to already existing target spanner table with record counts of source table

  @SPANNER_TEST @EXISTING_SPANNER_SINK
  Scenario: To verify data is getting transferred from Spanner source to Spanner sink having existing table
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Spanner" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Spanner" from the plugins list as: "Sink"
    Then Connect plugins: "Spanner" and "Spanner2" to establish connection
    Then Navigate to the properties page of plugin: "Spanner"
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "instanceId" with value: "spannerInstance"
    Then Enter input plugin property: "databaseName" with value: "spannerDatabase"
    Then Enter input plugin property: "tableName" with value: "spannerSourceTable"
    Then Validate output schema with expectedSchema "spannerSourceSchema"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Spanner2"
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "instanceId" with value: "spannerInstance"
    Then Enter input plugin property: "databaseName" with value: "spannerDatabase"
    Then Enter input plugin property: "tableName" with value: "spannerExistingTargetTable"
    Then Enter Spanner sink property primary key "spannerSinkPrimaryKeySpanner"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
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
    Then Validate records transferred to already existing target spanner table with record counts of source table

   @SPANNER_TEST @SPANNER_SINK_TEST
  Scenario: To verify data is getting transferred from no schema Spanner source to Spanner non existing sink table
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Spanner" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Spanner" from the plugins list as: "Sink"
    Then Connect plugins: "Spanner" and "Spanner2" to establish connection
    Then Navigate to the properties page of plugin: "Spanner"
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "instanceId" with value: "spannerInstance"
    Then Enter input plugin property: "databaseName" with value: "spannerDatabase"
    Then Enter input plugin property: "tableName" with value: "spannerSourceTable"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Spanner2"
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "instanceId" with value: "spannerInstance"
    Then Enter input plugin property: "databaseName" with value: "spannerTargetDatabase"
    Then Enter input plugin property: "tableName" with value: "spannerTargetTable"
    Then Enter Spanner sink property primary key "spannerSinkPrimaryKeySpanner"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
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
    Then Validate records transferred to target spanner table with record counts of source spanner table

