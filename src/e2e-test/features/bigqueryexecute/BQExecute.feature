@BQExecute
Feature: BigQueryExecute - Verify data transfer using BigQuery Execute plugin

  @BQ_SOURCE_TEST @BQ_SINK_TEST @BQ_EXECUTE_SQL @BQExecute_Required
  Scenario: Verify Store results in a BigQuery Table functionality of BQExecute plugin
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Execute" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Execute"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter textarea plugin property: "sql" with value: "bqExecuteQuery"
    Then Click plugin property: "storeResultsInBigQueryTable"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Override Service account details if set in environment variables
    Then Validate "BigQuery Execute" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target BigQuery Table: "bqTargetTable" is equal to number of records fetched from SQL query: "bqExecuteQuery"

  @BQ_SINK_TEST @BQ_SOURCE_BQ_EXECUTE_TEST @BQ_EXECUTE_ROW_AS_ARG_SQL
  Scenario: Verify Row As Arguments functionality of BQExecute plugin
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Execute" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Execute"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter textarea plugin property: "sql" with value: "bqExecuteRowAsArgQuery"
    Then Click plugin property: "rowAsArguments"
    Then Override Service account details if set in environment variables
    Then Validate "BigQuery Execute" plugin properties
    Then Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    Then Connect plugins: "BigQuery Execute" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Click on the Macro button of Property: "projectId" and set the value to: "bqExecuteArgProjectID"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "bqExecuteArgProjectID"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Click on the Macro button of Property: "dataset" and set the value to: "bqExecuteArgDataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Select Macro action of output schema property: "outputSchemaMacroInput" and set the value to "bqeSourceOutputSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery2" and "BigQuery3" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery3"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "BQSinkReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "truncateTable"
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "bqeSourceOutputSchema" for key "bqeSourceOutputSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"

  @BQ_EXECUTE_DDL_CREATE_TEST @BQExecute_Required
  Scenario: Verify BQExecute plugin functionality for DDL query - Create table
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Execute" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Execute"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter textarea plugin property: "sql" with value: "bqExecuteDDLCreate"
    Then Override Service account details if set in environment variables
    Then Validate "BigQuery Execute" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify BigQuery table: "bqExecuteCreateTable" is created

  @BQ_SOURCE_BQ_EXECUTE_TEST @BQ_EXECUTE_DDL_DROP_TEST
  Scenario: Verify BQExecute plugin functionality for DDL query - Drop table
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Execute" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Execute"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter textarea plugin property: "sql" with value: "bqExecuteDDLDrop"
    Then Override Service account details if set in environment variables
    Then Validate "BigQuery Execute" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify BigQuery table: "bqSourceTable" is deleted

  @BQ_SOURCE_BQ_EXECUTE_TEST @BQ_EXECUTE_INSERT_SQL @BQExecute_Required
  Scenario: Verify BQExecute plugin functionality for DML query - Insert data
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Execute" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Execute"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter textarea plugin property: "sql" with value: "bqExecuteDMLInsert"
    Then Override Service account details if set in environment variables
    Then Validate "BigQuery Execute" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify 1 records inserted in BigQuery table: "bqSourceTable" with query "bqExecuteCountDMLInsert"

  @BQ_SOURCE_TEST @BQ_EXECUTE_UPSERT_SQL
  Scenario: Verify BQExecute plugin functionality for DML query - Upsert data
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Execute" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Execute"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter textarea plugin property: "sql" with value: "bqExecuteDMLUpsert"
    Then Override Service account details if set in environment variables
    Then Validate "BigQuery Execute" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify 1 records inserted in BigQuery table: "bqSourceTable" with query "bqExecuteCountDMLUpsertInsert"
    Then Open pipeline actions menu
    Then Select pipeline action: "Duplicate"
    Then Verify plugin: "BigQuery Execute" node is displayed on the canvas with a timeout of 120 seconds
    Then Navigate to the properties page of plugin: "BigQuery Execute"
    Then Validate "BigQuery Execute" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Close the pipeline logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify 1 records updated in BigQuery table: "bqSourceTable" with query "bqExecuteCountDMLUpsertUpdate"

  @BQ_SOURCE_TEST @BQ_EXECUTE_UPDATE_SQL @BQExecute_Required
  Scenario: Verify BQExecute plugin functionality for DML query - Update data
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Execute" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Execute"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter textarea plugin property: "sql" with value: "bqExecuteDMLUpdate"
    Then Override Service account details if set in environment variables
    Then Validate "BigQuery Execute" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify 1 records updated in BigQuery table: "bqSourceTable" with query "bqExecuteCountDMLUpdate"
