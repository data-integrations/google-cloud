@BigQuery_Source
Feature: BigQuery source - Verification of BigQuery to GCS successful data transfer with macro arguments

  @CMEK @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to GCS with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property "projectId" as macro argument "bqProjectId"
    Then Enter BigQuery property "datasetProjectId" as macro argument "bqDatasetProjectId"
    Then Enter BigQuery property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter BigQuery property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter BigQuery property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter BigQuery property "dataset" as macro argument "bqDataset"
    Then Enter BigQuery property "table" as macro argument "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter GCS property reference name
    Then Enter GCS property "projectId" as macro argument "gcsProjectId"
    Then Enter GCS property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter GCS property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter GCS property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter GCS property "path" as macro argument "gcsSinkPath"
    Then Enter GCS sink property "pathSuffix" as macro argument "gcsPathSuffix"
    Then Enter GCS property "format" as macro argument "gcsFormat"
    Then Enter GCS sink cmek property "encryptionKeyName" as macro argument "cmekGCS" if cmek is enabled
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery source table name key "bqSourceTable"
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value for GCS sink property path key "gcsSinkPath"
    Then Enter runtime argument value "gcsPathDateSuffix" for key "gcsPathSuffix"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "cmekGCS" for GCS cmek property key "cmekGCS" if GCS cmek is enabled
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for GCS sink
    Then Close the preview data
    Then Deploy the pipeline
    Then Click on the Runtime Arguments Dropdown button
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery source table name key "bqSourceTable"
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value for GCS sink property path key "gcsSinkPath"
    Then Enter runtime argument value "gcsPathDateSuffix" for key "gcsPathSuffix"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "cmekGCS" for GCS cmek property key "cmekGCS" if GCS cmek is enabled
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
    Then Validate the cmek key "cmekGCS" of target GCS bucket if cmek is enabled

  @CMEK @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to GCS with macro arguments for partition start date and partition end date
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter BigQuery property reference name
    Then Enter BigQuery property "projectId" as macro argument "bqProjectId"
    Then Enter BigQuery property "datasetProjectId" as macro argument "bqDatasetProjectId"
    Then Enter BigQuery property "partitionFrom" as macro argument "bqStartDate"
    Then Enter BigQuery property "partitionTo" as macro argument "bqEndDate"
    Then Enter BigQuery property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter BigQuery property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter BigQuery property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter BigQuery property "dataset" as macro argument "bqDataset"
    Then Enter BigQuery property "table" as macro argument "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "GCS"
    Then Enter GCS property reference name
    Then Enter GCS property "projectId" as macro argument "gcsProjectId"
    Then Enter GCS property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter GCS property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter GCS property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter GCS property "path" as macro argument "gcsSinkPath"
    Then Enter GCS sink property "pathSuffix" as macro argument "gcsPathSuffix"
    Then Enter GCS property "format" as macro argument "gcsFormat"
    Then Enter GCS sink cmek property "encryptionKeyName" as macro argument "cmekGCS" if cmek is enabled
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "partitionFrom" for key "bqStartDate"
    Then Enter runtime argument value "partitionTo" for key "bqEndDate"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery source table name key "bqSourceTable"
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value for GCS sink property path key "gcsSinkPath"
    Then Enter runtime argument value "gcsPathDateSuffix" for key "gcsPathSuffix"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "cmekGCS" for GCS cmek property key "cmekGCS" if GCS cmek is enabled
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for GCS sink
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "partitionFrom" for key "bqStartDate"
    Then Enter runtime argument value "partitionTo" for key "bqEndDate"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery source table name key "bqSourceTable"
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value for GCS sink property path key "gcsSinkPath"
    Then Enter runtime argument value "gcsPathDateSuffix" for key "gcsPathSuffix"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "cmekGCS" for GCS cmek property key "cmekGCS" if GCS cmek is enabled
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
    Then Validate the cmek key "cmekGCS" of target GCS bucket if cmek is enabled

  @CMEK @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to GCS with macro arguments for filter and Output Schema
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Enter BigQuery property reference name
    Then Enter BigQuery property "projectId" as macro argument "bqProjectId"
    Then Enter BigQuery property "datasetProjectId" as macro argument "bqDatasetProjectId"
    Then Enter BigQuery property "filter" as macro argument "bqFilter"
    Then Enter BigQuery property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter BigQuery property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter BigQuery property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter BigQuery property "dataset" as macro argument "bqDataset"
    Then Enter BigQuery property "table" as macro argument "bqSourceTable"
    Then Select Macro action of output schema property: "Output Schema-macro-input" and set the value to "bqOutputSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "GCS"
    Then Enter GCS property reference name
    Then Enter GCS property "projectId" as macro argument "gcsProjectId"
    Then Enter GCS property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter GCS property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter GCS property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter GCS property "path" as macro argument "gcsSinkPath"
    Then Enter GCS sink property "pathSuffix" as macro argument "gcsPathSuffix"
    Then Enter GCS property "format" as macro argument "gcsFormat"
    Then Enter GCS sink cmek property "encryptionKeyName" as macro argument "cmekGCS" if cmek is enabled
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "filter" for key "bqFilter"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery source table name key "bqSourceTable"
    Then Enter runtime argument value "OutputSchema" for key "bqOutputSchema"
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value for GCS sink property path key "gcsSinkPath"
    Then Enter runtime argument value "gcsPathDateSuffix" for key "gcsPathSuffix"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "cmekGCS" for GCS cmek property key "cmekGCS" if GCS cmek is enabled
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for GCS sink
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "filter" for key "bqFilter"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery source table name key "bqSourceTable"
    Then Enter runtime argument value "OutputSchema" for key "bqOutputSchema"
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value for GCS sink property path key "gcsSinkPath"
    Then Enter runtime argument value "gcsPathDateSuffix" for key "gcsPathSuffix"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "cmekGCS" for GCS cmek property key "cmekGCS" if GCS cmek is enabled
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
    Then Validate the cmek key "cmekGCS" of target GCS bucket if cmek is enabled
