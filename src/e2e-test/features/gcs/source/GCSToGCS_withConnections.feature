@GCS_Source
Feature: GCS source - Verification of GCS to GCS successful data transfer using connections

  @GCS_CSV_TEST @GCS_SINK_TEST @GCS_CONNECTION
  Scenario: To verify data transfer from GCS to GCS with pipeline connection created from wrangler
    Given Open Wrangler connections page
    Then Click plugin property: "addConnection" button
    Then Click plugin property: "gcsConnectionRow"
    Then Enter input plugin property: "name" with value: "gcsConnectionName"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Override Service account details in Wrangler connection page if set in environment variables
    Then Click plugin property: "testConnection" button
    Then Verify the test connection is successful
    Then Click plugin property: "connectionCreate" button
    Then Verify the connection with name: "gcsConnectionName" is created successfully
    Then Select connection data row with name: "gcsSourceBucketName"
    Then Select connection data rows with path: "gcsCsvFile"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "parsingOptionConfirm" button
    Then Verify connection datatable is displayed for the data: "gcsCsvFile"
    Then Click Create Pipeline button and choose the type of pipeline as: "Batch pipeline"
    Then Verify plugin: "GCSFile" node is displayed on the canvas with a timeout of 120 seconds
    Then Navigate to the properties page of plugin: "GCSFile"
    Then Verify toggle plugin property: "useConnection" is toggled to: "YES"
    Then Verify plugin property: "connection" contains text: "gcsConnectionName"
    Then Verify input plugin property: "path" contains value: "gcsSourcePath"
    Then Verify dropdown plugin property: "format" is selected with option: "csv"
    Then Verify toggle plugin property: "skipHeader" is toggled to: "True"
    Then Validate output schema with expectedSchema "gcsCsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Wrangler"
    Then Verify the Output Schema matches the Expected Schema: "gcsCsvFileSchema"
    Then Validate "Wrangler" plugin properties
    Then Close the Plugin Properties page
    Then Expand Plugin group in the LHS plugins list: "Sink"
    Then Select plugin: "GCS" from the plugins list as: "Sink"
    Then Navigate to the properties page of plugin: "GCS2"
    Then Enter input plugin property: "referenceName" with value: "GCSSinkReferenceName"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Enter input plugin property: "path" with value: "gcsTargetPath"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Connect plugins: "Wrangler" and "GCS2" to establish connection
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
    Then Verify data is transferred to target GCS bucket
    Given Open Wrangler connections page
    Then Expand connections of type: "GCS"
    Then Open action menu for connection: "gcsConnectionName" of type: "GCS"
    Then Select action: "Delete" for connection: "gcsConnectionName" of type: "GCS"
    Then Click plugin property: "Delete" button
    Then Verify connection: "gcsConnectionName" of type: "GCS" is deleted successfully

  @GCS_CSV_TEST @GCS_SINK_TEST @EXISTING_GCS_CONNECTION @GCS_Source_Required
  Scenario: To verify data is getting transferred from GCS to GCS with use connection functionality
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Click on the Browse button inside plugin properties
    Then Select connection data row with name: "gcsSourceBucketName"
    Then Select connection data rows with path: "gcsCsvFile"
    Then Wait till connection data loading completes with a timeout of 60 seconds
    Then Verify input plugin property: "path" contains value: "gcsSourcePath"
    Then Click plugin property: "skipHeader"
    Then Validate output schema with expectedSchema "gcsCsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "GCS2"
    Then Enter input plugin property: "referenceName" with value: "GCSSinkReferenceName"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Enter input plugin property: "path" with value: "gcsTargetPath"
    Then Select dropdown plugin property: "format" with option value: "csv"
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
    Then Verify data is transferred to target GCS bucket
