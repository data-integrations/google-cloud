@GCS_Sink
Feature: GCS sink - Verification of GCS Sink plugin macro scenarios

  @BQ_SOURCE_DATATYPE_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to GCS sink with macro fields
    Given Open Datafusion Project to configure pipeline
    Then Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
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
    Then Enter GCS property "projectId" as macro argument "gcsProjectId"
    Then Enter GCS property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter GCS property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter GCS property "path" as macro argument "gcsSinkPath"
    Then Enter GCS sink property "pathSuffix" as macro argument "gcsPathSuffix"
    Then Enter GCS property "format" as macro argument "gcsFormat"
    Then Click on the Macro button of Property: "writeHeader" and set the value to: "WriteHeader"
    Then Click on the Macro button of Property: "location" and set the value to: "gcsSinkLocation"
    Then Click on the Macro button of Property: "contentType" and set the value to: "gcsContentType"
    Then Click on the Macro button of Property: "outputFileNameBase" and set the value to: "OutFileNameBase"
    Then Click on the Macro button of Property: "fileSystemProperties" and set the value to: "FileSystemPr"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value for GCS sink property path key "gcsSinkPath"
    Then Enter runtime argument value "gcsPathDateSuffix" for key "gcsPathSuffix"
    Then Enter runtime argument value "jsonFormat" for key "gcsFormat"
    Then Enter runtime argument value "writeHeader" for key "WriteHeader"
    Then Enter runtime argument value "contentType" for key "gcsContentType"
    Then Enter runtime argument value "gcsSinkBucketLocation" for key "gcsSinkLocation"
    Then Enter runtime argument value "outputFileNameBase" for key "OutFileNameBase"
    Then Enter runtime argument value "gcsCSVFileSysProperty" for key "FileSystemPr"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for GCS sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value for GCS sink property path key "gcsSinkPath"
    Then Enter runtime argument value "gcsPathDateSuffix" for key "gcsPathSuffix"
    Then Enter runtime argument value "jsonFormat" for key "gcsFormat"
    Then Enter runtime argument value "writeHeader" for key "WriteHeader"
    Then Enter runtime argument value "contentType" for key "gcsContentType"
    Then Enter runtime argument value "gcsSinkBucketLocation" for key "gcsSinkLocation"
    Then Enter runtime argument value "outputFileNameBase" for key "OutFileNameBase"
    Then Enter runtime argument value "gcsCSVFileSysProperty" for key "FileSystemPr"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
    Then Validate the values of records transferred to GCS bucket is equal to the values from source BigQuery table
