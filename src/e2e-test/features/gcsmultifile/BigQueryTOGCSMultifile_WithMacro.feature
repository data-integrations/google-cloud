@MultiFile
Feature: GCSMultiFile Sink - Verification of BigQuery to GCSMultiFile successful data transfer with macro arguments

  @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to GCSMultiFile with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCSMultiFile
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property "projectId" as macro argument "bqProjectId"
    Then Enter BigQuery property "datasetProjectId" as macro argument "bqDatasetProjectId"
    Then Enter BigQuery property "serviceAccountFilePath" as macro argument "bqServiceAccount"
    Then Enter BigQuery property "dataset" as macro argument "bqDataset"
    Then Enter BigQuery property "table" as macro argument "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property "projectId" as macro argument "gcsMultiFileProjectId"
    Then Enter GCSMultiFile property "serviceAccountFilePath" as macro argument "gcsMultiFileServiceAccount"
    Then Enter GCSMultiFile property "path" as macro argument "gcsMultiFileSinkPath"
    Then Enter GCSMultiFile property "pathSuffix" as macro argument "gcsMultiFilePathSuffix"
    Then Enter GCSMultiFile property "format" as macro argument "gcsMultiFileFormat"
    Then Toggle GCSMultiFile property allow flexible schemas to true
    Then Validate "GCSMultiFiles" plugin properties
    Then Close GCSMultiFile Properties
    Then Connect source as "BigQuery" and sink as "GCSMultiFiles" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountAutoDetect" for key "bqServiceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery source table name key "bqSourceTable"
    Then Enter runtime argument value "projectId" for key "gcsMultiFileProjectId"
    Then Enter runtime argument value "serviceAccountAutoDetect" for key "gcsMultiFileServiceAccount"
    Then Enter runtime argument value for GCSMultiFile property path key "gcsMultiFileSinkPath"
    Then Enter runtime argument value "multiFilePathSuffix" for key "gcsMultiFilePathSuffix"
    Then Enter runtime argument value "csvFormat" for key "gcsMultiFileFormat"
    Then Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "success"
    Then Click on preview data for GCS sink
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountAutoDetect" for key "bqServiceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery source table name key "bqSourceTable"
    Then Enter runtime argument value "projectId" for key "gcsMultiFileProjectId"
    Then Enter runtime argument value "serviceAccountAutoDetect" for key "gcsMultiFileServiceAccount"
    Then Enter runtime argument value for GCSMultiFile property path key "gcsMultiFileSinkPath"
    Then Enter runtime argument value "gcsPathDateSuffix" for key "gcsMultiFilePathSuffix"
    Then Enter runtime argument value "csvFormat" for key "gcsMultiFileFormat"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
