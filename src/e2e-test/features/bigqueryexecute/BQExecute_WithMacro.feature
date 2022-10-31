@BQExecute
Feature: BigQueryExecute - Verify data transfer using BigQuery Execute plugin with macro arguments

  @BQ_SOURCE_TEST @BQ_SINK_TEST @BQ_EXECUTE_SQL
  Scenario:Verify Store results in a BigQuery Table functionality of BQExecute plugin using macro arguments
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Execute" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Execute"
    Then Click on the Macro button of Property: "projectId" and set the value to: "bqeProjectId"
    Then Click on the Macro button of Property: "sql" and set the value in textarea: "bqeSQl"
    Then Click plugin property: "storeResultsInBigQueryTable"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value to: "bqeDatasetProjectId"
    Then Click on the Macro button of Property: "dataset" and set the value to: "bqeDataset"
    Then Click on the Macro button of Property: "table" and set the value to: "bqeTable"
    Then Click on the Macro button of Property: "serviceAccountType" and set the value to: "serviceAccountType"
    Then Click on the Macro button of Property: "serviceAccountFilePath" and set the value to: "serviceAccount"
    Then Click on the Macro button of Property: "serviceAccountJSON" and set the value to: "serviceAccount"
    Then Validate "BigQuery Execute" plugin properties
    Then Close the Plugin Properties page
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "bqeProjectId"
    Then Enter runtime argument value "bqExecuteQuery" for key "bqeSQl"
    Then Enter runtime argument value "projectId" for key "bqeDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqeDataset"
    Then Enter runtime argument value "bqTargetTable" for key "bqeTable"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target BigQuery Table: "bqTargetTable" is equal to number of records fetched from SQL query: "bqExecuteQuery"
