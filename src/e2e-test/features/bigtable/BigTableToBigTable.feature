@BigTable
Feature: BigTable source - Verification of BigTable to BigTable successful data transfer

  @BIGTABLE_SOURCE_TEST @BIGTABLE_SINK_TEST
  Scenario: To verify data is getting transferred from BigTable source table to BigTable sink table
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Bigtable" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Bigtable" from the plugins list as: "Sink"
    Then Connect plugins: "Bigtable" and "Bigtable2" to establish connection
    Then Navigate to the properties page of plugin: "Bigtable"
    Then Enter input plugin property: "referenceName" with value: "CBTSourceReferenceName"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "instance" with value: "bigtableInstance"
    Then Enter input plugin property: "table" with value: "bigtableSourceTable"
    Then Replace input plugin property: "keyAlias" with value: "id"
    Then Enter key value pairs for plugin property: "columnMappings" with values from json: "cbtsourceMappings"
    Then Select Macro action of output schema property: "outputSchemaMacroInput" and set the value to "cbtSourceOutputSchema"
    Then Validate "Bigtable" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Bigtable2"
    Then Enter input plugin property: "referenceName" with value: "CBTSinkReferenceName"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "instance" with value: "bigtableTargetInstance"
    Then Enter input plugin property: "table" with value: "bigtableTargetTable"
    Then Replace input plugin property: "keyAlias" with value: "id"
    Then Enter key value pairs for plugin property: "columnMappings" with values from json: "cbtsinkMappings"
    Then Validate "Bigtable" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "cbtSourceOutputSchema" for key "cbtSourceOutputSchema"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "cbtSourceOutputSchema" for key "cbtSourceOutputSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Validate data transferred to target bigtable table with data of source bigtable table

  @BIGTABLE_SOURCE_TEST @EXISTING_BIGTABLE_SINK
  Scenario: To verify data is getting transferred from BigTable source table to existing BigTable sink
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Bigtable" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Bigtable" from the plugins list as: "Sink"
    Then Connect plugins: "Bigtable" and "Bigtable2" to establish connection
    Then Navigate to the properties page of plugin: "Bigtable"
    Then Enter input plugin property: "referenceName" with value: "CBTSourceReferenceName"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "instance" with value: "bigtableInstance"
    Then Enter input plugin property: "table" with value: "bigtableSourceTable"
    Then Replace input plugin property: "keyAlias" with value: "id"
    Then Enter key value pairs for plugin property: "columnMappings" with values from json: "cbtsourceMappings"
    Then Select Macro action of output schema property: "outputSchemaMacroInput" and set the value to "cbtSourceOutputSchema"
    Then Validate "Bigtable" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Bigtable2"
    Then Enter input plugin property: "referenceName" with value: "CBTSinkReferenceName"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "instance" with value: "bigtableTargetInstance"
    Then Enter input plugin property: "table" with value: "bigtableTargetExistingTable"
    Then Replace input plugin property: "keyAlias" with value: "id"
    Then Enter key value pairs for plugin property: "columnMappings" with values from json: "cbtsinkMappings"
    Then Validate "Bigtable" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "cbtSourceOutputSchema" for key "cbtSourceOutputSchema"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "cbtSourceOutputSchema" for key "cbtSourceOutputSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Validate data transferred to existing target bigtable table with data of source bigtable table

  @BIGTABLE_SOURCE_TEST @BIGTABLE_SINK_TEST
  Scenario: To verify data is getting transferred from not existing BigTable source table to BigTable sink table
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Bigtable" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Bigtable" from the plugins list as: "Sink"
    Then Connect plugins: "Bigtable" and "Bigtable2" to establish connection
    Then Navigate to the properties page of plugin: "Bigtable"
    Then Enter input plugin property: "referenceName" with value: "CBTSourceReferenceName"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "instance" with value: "bigtableInstance"
    Then Enter input plugin property: "table" with value: "bigtableSourceTable"
    Then Replace input plugin property: "keyAlias" with value: "id"
    Then Enter key value pairs for plugin property: "columnMappings" with values from json: "cbtsourceMappings"
    Then Select Macro action of output schema property: "outputSchemaMacroInput" and set the value to "cbtSourceOutputSchema"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Bigtable2"
    Then Enter input plugin property: "referenceName" with value: "CBTSinkReferenceName"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "instance" with value: "bigtableTargetInstance"
    Then Enter input plugin property: "table" with value: "bigtableTargetTable"
    Then Replace input plugin property: "keyAlias" with value: "id"
    Then Enter key value pairs for plugin property: "columnMappings" with values from json: "cbtsinkMappings"
    Then Validate "Bigtable" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "cbtSourceOutputSchema" for key "cbtSourceOutputSchema"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "cbtSourceOutputSchema" for key "cbtSourceOutputSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Validate data transferred to target bigtable table with data of source bigtable table

