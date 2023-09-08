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

  @GCS_CSV @GCS_SINK_TEST @EXISTING_GCS_CONNECTION
  Scenario: To verify data is getting transferred from GCS Source to GCS Sink using test Schema Detection On Single File with connection
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Select dropdown plugin property: "select-schema-actions-dropdown" with option value: "clear"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS source property path "gcsCsvDataFile"
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "delimiterValue"
    Then Toggle GCS source property skip header to true
    Then Validate "GCS" plugin properties
    Then Verify the Output Schema matches the Expected Schema: "gcsSingleFileDataSchema"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "GCS2"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS sink property path
    Then Select dropdown plugin property: "select-format" with option value: "csv"
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
    Then Validate the data from GCS Source to GCS Sink with expected csv file and target data in GCS bucket

  @GCS_CSV @GCS_SINK_TEST
  Scenario: To verify data is getting transferred from GCS Source to GCS Sink using test Schema Detection On Single File without connection
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Select dropdown plugin property: "select-schema-actions-dropdown" with option value: "clear"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS source property path "gcsCsvDataFile"
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "delimiterValue"
    Then Toggle GCS source property skip header to true
    Then Validate "GCS" plugin properties
    Then Verify the Output Schema matches the Expected Schema: "gcsSingleFileDataSchema"
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "GCS2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS sink property path
    Then Select dropdown plugin property: "select-format" with option value: "csv"
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
    Then Validate the data from GCS Source to GCS Sink with expected csv file and target data in GCS bucket

  @GCS_CSV @GCS_SINK_TEST
  Scenario: To verify the pipeline is getting failed from GCS to GCS when Schema is not cleared in GCS source On Single File
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS source property path "gcsCsvDataFile"
    Then Select GCS property format "csv"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "GCS2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS sink property path
    Then Select dropdown plugin property: "select-format" with option value: "csv"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Failed"

  @GCS_MULTIPLE_FILES_TEST @GCS_SINK_TEST @EXISTING_GCS_CONNECTION
  Scenario: To verify the pipeline is getting failed from GCS Source to GCS Sink On Multiple File with connection
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Select dropdown plugin property: "select-schema-actions-dropdown" with option value: "clear"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS source property path "gcsMultipleFilesPath"
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "delimiterValue"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "GCS2"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Enter input plugin property: "referenceName" with value: "sinkRef"
    Then Enter GCS sink property path
    Then Select dropdown plugin property: "select-format" with option value: "json"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                                  |
      | ERROR | errorMessageMultipleFileWithFirstRowAsHeaderDisabled     |

  @GCS_MULTIPLE_FILES_TEST @GCS_SINK_TEST
  Scenario: To verify the pipeline is getting failed from GCS Source to GCS Sink On Multiple File without connection
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Select dropdown plugin property: "select-schema-actions-dropdown" with option value: "clear"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS source property path "gcsMultipleFilesPath"
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "delimiterValue"
    Then Toggle GCS source property skip header to true
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
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                                     |
      | ERROR | errorMessageMultipleFileWithFirstRowAsHeaderEnabled         |

  @GCS_MULTIPLE_FILES_TEST @GCS_SINK_TEST
  Scenario: To verify the pipeline is getting failed from GCS to GCS when Schema is not cleared in GCS source On Multiple File
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS source property path "gcsMultipleFilesPath"
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "delimiterValue"
    Then Toggle GCS source property skip header to true
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
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                                      |
      | ERROR | errorMessageMultipleFileWithoutClearDefaultSchema            |

  @GCS_MULTIPLE_FILES_REGEX_TEST @GCS_SINK_TEST @EXISTING_GCS_CONNECTION
  Scenario: To verify the pipeline is getting failed from GCS to GCS On Multiple File with filter regex using connection
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Select dropdown plugin property: "select-schema-actions-dropdown" with option value: "clear"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS source property path "gcsMultipleFilesFilterRegexPath"
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "delimiterValue"
    Then Toggle GCS source property skip header to true
    Then Enter input plugin property: "fileRegex" with value: "fileRegexValue"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "GCS2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
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
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data from GCS Source to GCS Sink with expected json file and target data in GCS bucket

  @GCS_MULTIPLE_FILES_REGEX_TEST @GCS_SINK_TEST
  Scenario: To verify the pipeline is getting failed from GCS to GCS On Multiple File with filter regex without using connection
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Select dropdown plugin property: "select-schema-actions-dropdown" with option value: "clear"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter GCS source property path "gcsMultipleFilesFilterRegexPath"
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "delimiterValue"
    Then Toggle GCS source property skip header to true
    Then Enter input plugin property: "fileRegex" with value: "fileRegexValue"
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
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the data from GCS Source to GCS Sink with expected json file and target data in GCS bucket
