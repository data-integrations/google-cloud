@Spanner_Source @SPANNER_TEST
Feature: Spanner source - Verification of Spanner to Spanner successful data transfer using connections

  @SPANNER_SOURCE_BASIC_TEST @SPANNER_SINK_TEST @SPANNER_CONNECTION @Spanner_Source_Required
  Scenario: To verify data transfer from Spanner to Spanner with pipeline connection created from wrangler
    Given Open Wrangler connections page
    Then Click plugin property: "addConnection" button
    Then Click plugin property: "spannerConnectionRow"
    Then Enter input plugin property: "name" with value: "spannerConnectionName"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Override Service account details in Wrangler connection page if set in environment variables
    Then Click plugin property: "testConnection" button
    Then Verify the test connection is successful
    Then Click plugin property: "connectionCreate" button
    Then Verify the connection with name: "spannerConnectionName" is created successfully
    Then Select connection data row with name: "spannerInstance"
    Then Select connection data row with name: "spannerDatabase"
    Then Select connection data row with name: "spannerSourceBasicTable"
    Then Verify connection datatable is displayed for the data: "spannerSourceBasicTable"
    Then Click Create Pipeline button and choose the type of pipeline as: "Batch pipeline"
    Then Verify plugin: "Spanner" node is displayed on the canvas with a timeout of 120 seconds
    Then Navigate to the properties page of plugin: "Spanner"
    Then Verify toggle plugin property: "useConnection" is toggled to: "YES"
    Then Verify plugin property: "connection" contains text: "spannerConnectionName"
    Then Verify input plugin property: "instanceId" contains value: "spannerInstance"
    Then Verify input plugin property: "databaseName" contains value: "spannerDatabase"
    Then Verify input plugin property: "tableName" contains value: "spannerSourceTable"
    Then Validate output schema with expectedSchema "spannerSourceBasicSchema"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Wrangler"
    Then Verify the Output Schema matches the Expected Schema: "spannerSourceBasicSchema"
    Then Validate "Wrangler" plugin properties
    Then Close the Plugin Properties page
    Then Expand Plugin group in the LHS plugins list: "Sink"
    Then Select plugin: "Spanner" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "Spanner2"
    Then Enter input plugin property: "referenceName" with value: "SpannerSinkReferenceName"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "spannerConnectionName"
    Then Enter input plugin property: "instanceId" with value: "spannerInstance"
    Then Enter input plugin property: "databaseName" with value: "spannerTargetDatabase"
    Then Enter input plugin property: "tableName" with value: "spannerTargetTable"
    Then Enter Spanner sink property primary key "spannerSinkPrimaryKeySpanner"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
    Then Connect plugins: "Wrangler" and "Spanner2" to establish connection
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
    Then Validate records transferred to target spanner table with record counts of source spanner table
    Given Open Wrangler connections page
    Then Expand connections of type: "Spanner"
    Then Open action menu for connection: "spannerConnectionName" of type: "Spanner"
    Then Select action: "Delete" for connection: "spannerConnectionName" of type: "Spanner"
    Then Click plugin property: "Delete" button
    Then Verify connection: "spannerConnectionName" of type: "Spanner" is deleted successfully

  @SPANNER_SINK_TEST @EXISTING_SPANNER_CONNECTION @Spanner_Source_Required
  Scenario: To verify data is getting transferred from Spanner to Spanner with use connection functionality
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Spanner" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Spanner" from the plugins list as: "Sink"
    Then Connect plugins: "Spanner" and "Spanner2" to establish connection
    Then Navigate to the properties page of plugin: "Spanner"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "spannerConnectionName"
    Then Click on the Browse button inside plugin properties
    Then Select connection data row with name: "spannerInstance"
    Then Select connection data row with name: "spannerDatabase"
    Then Select connection data row with name: "spannerSourceTable"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "instanceId" contains value: "spannerInstance"
    Then Verify input plugin property: "databaseName" contains value: "spannerDatabase"
    Then Verify input plugin property: "tableName" contains value: "spannerSourceTable"
    Then Validate output schema with expectedSchema "spannerSourceSchema"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Spanner2"
    Then Enter input plugin property: "referenceName" with value: "SpannerSinkReferenceName"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "spannerConnectionName"
    Then Click on the Browse button inside plugin properties
    Then Click SELECT button inside connection data row with name: "spannerInstance"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "instanceId" contains value: "spannerInstance"
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

  @SPANNER_SINK_TEST @EXISTING_SPANNER_CONNECTION
  Scenario: To verify data is getting transferred from Spanner to Spanner with use connection functionality and browsing connections partially till database
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Spanner" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Spanner" from the plugins list as: "Sink"
    Then Connect plugins: "Spanner" and "Spanner2" to establish connection
    Then Navigate to the properties page of plugin: "Spanner"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "spannerConnectionName"
    Then Enter input plugin property: "referenceName" with value: "SpannerSourceReferenceName"
    Then Click on the Browse button inside plugin properties
    Then Select connection data row with name: "spannerInstance"
    Then Click SELECT button inside connection data row with name: "spannerDatabase"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "instanceId" contains value: "spannerInstance"
    Then Verify input plugin property: "databaseName" contains value: "spannerDatabase"
    Then Enter input plugin property: "tableName" with value: "spannerSourceTable"
    Then Validate output schema with expectedSchema "spannerSourceSchema"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Spanner2"
    Then Enter input plugin property: "referenceName" with value: "SpannerSinkReferenceName"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "spannerConnectionName"
    Then Click on the Browse button inside plugin properties
    Then Click SELECT button inside connection data row with name: "spannerInstance"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "instanceId" contains value: "spannerInstance"
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

  @EXISTING_SPANNER_SINK @EXISTING_SPANNER_CONNECTION @Spanner_Source_Required
  Scenario: To verify data is getting transferred from Spanner source  to existing Spanner sink with use connection functionality
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Spanner" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Spanner" from the plugins list as: "Sink"
    Then Connect plugins: "Spanner" and "Spanner2" to establish connection
    Then Navigate to the properties page of plugin: "Spanner"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "spannerConnectionName"
    Then Click on the Browse button inside plugin properties
    Then Select connection data row with name: "spannerInstance"
    Then Select connection data row with name: "spannerDatabase"
    Then Select connection data row with name: "spannerSourceTable"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "instanceId" contains value: "spannerInstance"
    Then Verify input plugin property: "databaseName" contains value: "spannerDatabase"
    Then Verify input plugin property: "tableName" contains value: "spannerSourceTable"
    Then Validate output schema with expectedSchema "spannerSourceSchema"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Spanner2"
    Then Enter input plugin property: "referenceName" with value: "SpannerSinkReferenceName"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "spannerConnectionName"
    Then Click on the Browse button inside plugin properties
    Then Click SELECT button inside connection data row with name: "spannerInstance"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "instanceId" contains value: "spannerInstance"
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
    Then Validate records transferred to existing target spanner table with record counts of  source spanner table