@GCS_Source
Feature: GCS source - Verification of GCS to GCS Additional Tests successful

  @GCS_AVRO_FILE @GCS_SINK_TEST
  Scenario: To verify data is getting transferred from GCS to GCS using Avro and Json file format with different data types
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS source property path "gcsAvroAllDataFile"
    Then Select GCS property format "avro"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "gcsAvroAllTypeDataSchema"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "GCS2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS sink property path
    Then Select dropdown plugin property: "select-format" with option value: "json"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
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
    Then Validate the data transferred from GCS Source to GCS Sink with Expected avro file and target data in GCS bucket

  @GCS_AVRO_FILE @GCS_SINK_TEST @EXISTING_GCS_CONNECTION
  Scenario: To verify data is getting transferred from GCS to GCS using Avro and Json file format with different data types using connection
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS source property path "gcsAvroAllDataFile"
    Then Select GCS property format "avro"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "gcsAvroAllTypeDataSchema"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "GCS2"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS sink property path
    Then Select dropdown plugin property: "select-format" with option value: "json"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
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
    Then Validate the data transferred from GCS Source to GCS Sink with Expected avro file and target data in GCS bucket
