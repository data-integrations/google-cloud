@BigQueryArgumentSetter
Feature: BigQuery Argument Setter - e2e data transfer with source and sink

  @BQ_ARGUMENT_SETTER_TEST @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Verify BigQuery Argument Setter plugin functionality using BigQuery to BigQuery pipeline
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Argument Setter" from the plugins list as: "Conditions and Actions"
    Then Connect plugins: "BigQuery Argument Setter" and "BigQuery2" to establish connection
    Then Connect plugins: "BigQuery2" and "BigQuery" to establish connection
    When Navigate to the properties page of plugin: "BigQuery Argument Setter"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "datasetProjectId" with value: "projectId"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Enter key value pairs for plugin property: "bqArgumentSetterConditionField" with values from json: "bqArgumentSetterConditionName"
    Then Enter the Arguments Columns field as list "bqArgumentSetterColumnValue"
    Then Override Service account details if set in environment variables
    Then Click on the Validate button
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "bqArgumentSetterReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "bqArgumentSetterReferenceNameSink"
    Then Click on the Macro button of Property: "dataset" and set the value to: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target table is equal to number of records from source table

  @BQ_ARGUMENT_SETTER_TEST @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Verify BigQuery Argument Setter plugin functionality with multiple arguments using BigQuery to BigQuery pipeline
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Argument Setter" from the plugins list as: "Conditions and Actions"
    Then Connect plugins: "BigQuery Argument Setter" and "BigQuery2" to establish connection
    Then Connect plugins: "BigQuery2" and "BigQuery" to establish connection
    When Navigate to the properties page of plugin: "BigQuery Argument Setter"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "datasetProjectId" with value: "projectId"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Enter key value pairs for plugin property: "bqArgumentSetterConditionField" with values from json: "bqArgumentSetterConditionName2"
    Then Enter the Arguments Columns field as list "bqArgumentSetterColumnValue"
    Then Override Service account details if set in environment variables
    Then Click on the Validate button
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "bqArgumentSetterReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "bqArgumentSetterReferenceNameSink"
    Then Click on the Macro button of Property: "dataset" and set the value to: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target table is equal to number of records from source table

  @BQ_ARGUMENT_SETTER_TEST @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Verify BigQuery Argument Setter e2e data transfer with Invalid Values
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Argument Setter" from the plugins list as: "Conditions and Actions"
    Then Connect plugins: "BigQuery Argument Setter" and "BigQuery2" to establish connection
    Then Connect plugins: "BigQuery2" and "BigQuery" to establish connection
    When Navigate to the properties page of plugin: "BigQuery Argument Setter"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Replace input plugin property: "datasetProjectId" with value: "projectId"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Enter key value pairs for plugin property: "bqArgumentSetterConditionField" with values from json: "bqArgumentSetterConditionInvalidName"
    Then Enter the Arguments Columns field as list "bqArgumentSetterColumnValue"
    Then Override Service account details if set in environment variables
    Then Click on the Validate button
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "bqArgumentSetterReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    When Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "bqArgumentSetterReferenceNameSink"
    Then Click on the Macro button of Property: "dataset" and set the value to: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Failed"
