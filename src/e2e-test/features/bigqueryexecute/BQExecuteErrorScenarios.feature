@BQExecute
Feature: BigQueryExecute - Verify BigQueryExecute plugin error scenarios

  Scenario: Verify BigQueryExecute validation error for mandatory field SQL Query
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Execute" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Execute"
    Then Validate mandatory property error for "sql"

  @BQ_SOURCE_BQ_EXECUTE_TEST @BQ_EXECUTE_SQL
  Scenario: Verify BigQueryExecute validation error for mandatory field job location
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Execute" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Execute"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter textarea plugin property: "sql" with value: "bqExecuteQuery"
    Then Replace input plugin property: "location" with value: ""
    Then Validate mandatory property error for "location"

  @BQ_SOURCE_BQ_EXECUTE_TEST @BQ_SINK_TEST @BQ_EXECUTE_SQL
  Scenario: Verify BigQueryExecute validation error for empty datasetName
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Execute" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Execute"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter textarea plugin property: "sql" with value: "bqExecuteQuery"
    Then Click plugin property: "storeResultsInBigQueryTable"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "dataset" is displaying an in-line error message: "errorMessageBQExecuteTableDataset"

  @BQ_SOURCE_BQ_EXECUTE_TEST @BQ_SINK_TEST @BQ_EXECUTE_SQL
  Scenario: Verify BigQueryExecute validation error for empty table name
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Execute" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Execute"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter textarea plugin property: "sql" with value: "bqExecuteQuery"
    Then Click plugin property: "storeResultsInBigQueryTable"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "table" is displaying an in-line error message: "errorMessageBQExecuteTableDataset"
