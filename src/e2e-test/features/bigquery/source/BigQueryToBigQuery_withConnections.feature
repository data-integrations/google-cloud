@BigQuery_Source
Feature: BigQuery source - Verification of BigQuery to BigQuery successful data transfer using connections

  @BQ_SOURCE_TEST @BQ_SINK_TEST @BQ_CONNECTION @BigQuery_Source_Required
  Scenario: To verify data transfer from BigQuery to BigQuery with pipeline connection created from wrangler
    Given Open Wrangler connections page
    Then Click plugin property: "addConnection" button
    Then Click plugin property: "bqConnectionRow"
    Then Enter input plugin property: "name" with value: "bqConnectionName"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details in Wrangler connection page if set in environment variables
    Then Click plugin property: "testConnection" button
    Then Verify the test connection is successful
    Then Click plugin property: "connectionCreate" button
    Then Verify the connection with name: "bqConnectionName" is created successfully
    Then Select connection data row with name: "dataset"
    Then Select connection data row with name: "bqSourceTable"
    Then Verify connection datatable is displayed for the data: "bqSourceTable"
    Then Click Create Pipeline button and choose the type of pipeline as: "Batch pipeline"
    Then Verify plugin: "BigQueryTable" node is displayed on the canvas with a timeout of 120 seconds
    Then Navigate to the properties page of plugin: "BigQueryTable"
    Then Verify toggle plugin property: "useConnection" is toggled to: "YES"
    Then Verify plugin property: "connection" contains text: "bqConnectionName"
    Then Verify input plugin property: "dataset" contains value: "dataset"
    Then Verify input plugin property: "table" contains value: "bqSourceTable"
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Wrangler"
    Then Verify the Output Schema matches the Expected Schema: "bqSourceSchema"
    Then Validate "Wrangler" plugin properties
    Then Close the Plugin Properties page
    Then Expand Plugin group in the LHS plugins list: "Sink"
    Then Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "truncateTable"
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Connect plugins: "Wrangler" and "BigQuery2" to establish connection
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
    Then Validate records transferred to target table is equal to number of records from source table
    Given Open Wrangler connections page
    Then Expand connections of type: "BigQuery"
    Then Open action menu for connection: "bqConnectionName" of type: "BigQuery"
    Then Select action: "Delete" for connection: "bqConnectionName" of type: "BigQuery"
    Then Click plugin property: "Delete" button
    Then Verify connection: "bqConnectionName" of type: "BigQuery" is deleted successfully

  @BQ_SOURCE_TEST @BQ_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario: To verify data is getting transferred from BigQuery to BigQuery with use connection functionality
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Click on the Browse button inside plugin properties
    Then Select connection data row with name: "dataset"
    Then Select connection data row with name: "bqSourceTable"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "dataset" contains value: "dataset"
    Then Verify input plugin property: "table" contains value: "bqSourceTable"
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Click on the Browse button inside plugin properties
    Then Click SELECT button inside connection data row with name: "dataset"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "dataset" contains value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "truncateTable"
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery" plugin properties
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
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target table is equal to number of records from source table

  @BQ_SOURCE_TEST @BQ_SINK_TEST @EXISTING_BQ_CONNECTION
  Scenario: To verify data is getting transferred from BigQuery to BigQuery with use connection functionality and browsing connections partially till dataset
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Browse button inside plugin properties
    Then Click SELECT button inside connection data row with name: "dataset"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "dataset" contains value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "bqConnectionName"
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Click on the Browse button inside plugin properties
    Then Click SELECT button inside connection data row with name: "dataset"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "dataset" contains value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "truncateTable"
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery" plugin properties
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
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target table is equal to number of records from source table
