@Spanner_Sources
Feature: Spanner source - Verification of Spanner to Spanner Successful data transfer with macro arguments

  @SPANNER_SINK_TEST @SPANNER_TEST
  Scenario: To verify data is getting transferred from Spanner to Spanner with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Spanner" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Spanner" from the plugins list as: "Sink"
    Then Connect plugins: "Spanner" and "Spanner2" to establish connection
    Then Navigate to the properties page of plugin: "Spanner"
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Click on the Macro button of Property: "instanceId" and set the value to: "macroStringInstance"
    Then Click on the Macro button of Property: "databaseName" and set the value to: "macroStringDatabase"
    Then Click on the Macro button of Property: "tableName" and set the value to: "macroStringSourceTable"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Spanner2"
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Click on the Macro button of Property: "instanceId" and set the value to: "macroStringInstance"
    Then Click on the Macro button of Property: "databaseName" and set the value to: "macroStringTargetDatabase"
    Then Click on the Macro button of Property: "tableName" and set the value to: "macroStringTargetTable"
    Then Click on the Macro button of Property: "keys" and set the value to: "macroStringPrimaryKey"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "spannerInstance" for key "macroStringInstance"
    Then Enter runtime argument value "spannerDatabase" for key "macroStringDatabase"
    Then Enter runtime argument value "spannerSourceTable" for key "macroStringSourceTable"
    Then Enter runtime argument value "spannerTargetDatabase" for key "macroStringTargetDatabase"
    Then Enter runtime argument value "spannerTargetTable" for key "macroStringTargetTable"
    Then Enter runtime argument value "spannerSinkPrimaryKeySpanner" for key "macroStringPrimaryKey"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "spannerInstance" for key "macroStringInstance"
    Then Enter runtime argument value "spannerDatabase" for key "macroStringDatabase"
    Then Enter runtime argument value "spannerSourceTable" for key "macroStringSourceTable"
    Then Enter runtime argument value "spannerTargetDatabase" for key "macroStringTargetDatabase"
    Then Enter runtime argument value "spannerTargetTable" for key "macroStringTargetTable"
    Then Enter runtime argument value "spannerSinkPrimaryKeySpanner" for key "macroStringPrimaryKey"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target spanner table with record counts of source spanner table