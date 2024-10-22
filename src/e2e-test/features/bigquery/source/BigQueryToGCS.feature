@BigQuery_Sources
Feature: BigQuery source - Verification of BigQuery to GCS successful data transfer

  @CMEK @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to GCS
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Override Service account details if set in environment variables
    Then Enter BiqQuery property encryption key name "cmekBQ" if cmek is enabled
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS sink mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for GCS sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket

  @BQ_SOURCE_DATATYPE_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery with different datatypes to GCS
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Validate output schema with expectedSchema "bqSourceSchemaDatatype"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS sink mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for GCS sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
    Then Validate the values of records transferred to GCS bucket is equal to the values from source BigQuery table

  @BQ_SOURCE_TEST @BQ_SOURCE_VIEW_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to GCS by enable querying views
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name as view
    Then Toggle BigQuery source property enable querying views to true
    Then Enter the BigQuery source property for view materialization project "projectId"
    Then Enter the BigQuery source property for view materialization dataset "dataset"
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS sink mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for GCS sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket

@BQ_SOURCE_DATATYPE_TEST @GCS_SINK_MULTI_PART_UPLOAD
Scenario:Validate successful records transfer from BigQuery to GCS with bucket having delete multi part upload policy enabled
  Given Open Datafusion Project to configure pipeline
  When Expand Plugin group in the LHS plugins list: "Source"
  When Select plugin: "BigQuery" from the plugins list as: "Source"
  When Expand Plugin group in the LHS plugins list: "Sink"
  When Select plugin: "GCS" from the plugins list as: "Sink"
  Then Navigate to the properties page of plugin: "BigQuery"
  And Enter input plugin property: "referenceName" with value: "Reference"
  And Replace input plugin property: "project" with value: "projectId"
  And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
  And Replace input plugin property: "dataset" with value: "dataset"
  Then Override Service account details if set in environment variables
  And Enter input plugin property: "table" with value: "bqSourceTable"
  Then Click on the Get Schema button
  Then Validate output schema with expectedSchema "bqSourceSchemaDatatype"
  Then Validate "BigQuery" plugin properties
  Then Close the BigQuery properties
  Then Navigate to the properties page of plugin: "GCS"
  Then Enter input plugin property: "referenceName" with value: "sourceRef"
  Then Replace input plugin property: "project" with value: "projectId"
  Then Enter GCS sink property path
  Then Select dropdown plugin property: "select-format" with option value: "json"
  Then Validate "GCS" plugin properties
  Then Close the Plugin Properties page
  Then Connect source as "BigQuery" and sink as "GCS" to establish connection
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
  Then Validate the values of records transferred to GCS bucket is equal to the values from source BigQuery table
