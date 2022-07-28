@BQMT_Sink
Feature: BQMT-Sink - Verify GCS to BQMT sink plugin data transfer scenarios with macro arguments

  @CMEK @GCS_CSV_BQMT_TEST @BQ_SINK_TEST_CSV_BQMT
  Scenario:Validate successful records transfer from GCS to BQMT sink using advance fields with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BiqQueryMultiTable
    Then Open GCS source properties
    Then Enter GCS property reference name
    Then Enter GCS property "projectId" as macro argument "gcsProjectId"
    Then Enter GCS property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter GCS property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter GCS property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter GCS property "path" as macro argument "gcsSourcePath"
    Then Enter GCS property "format" as macro argument "gcsFormat"
    Then Enter GCS source property "skipHeader" as macro argument "gcsSkipHeader"
    Then Enter GCS source property output schema "outputSchema" as macro argument "gcsOutputSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BigQueryMultiTable sink property "projectId" as macro argument "bqProjectId"
    Then Enter BigQueryMultiTable sink property "datasetProjectId" as macro argument "bqDatasetProjectId"
    Then Enter BigQueryMultiTable sink property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter BigQueryMultiTable sink property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter BigQueryMultiTable sink property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter BigQueryMultiTable sink property "dataset" as macro argument "bqDataset"
    Then Enter BigQueryMultiTable sink cmek property "encryptionKeyName" as macro argument "cmekBQ" if cmek is enabled
    Then Enter BigQueryMultiTable sink property "truncateTable" as macro argument "bqTruncateTable"
    Then Toggle BiqQueryMultiTable sink property allow flexible schema to "true"
    Then Enter BigQueryMultiTable sink property "allowSchemaRelaxation" as macro argument "bqmtUpdateTableSchema"
    Then Validate "BigQueryMultiTable" plugin properties
    Then Close the BiqQueryMultiTable properties
    Then Connect source as "GCS" and sink as "BigQueryMultiTable" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "bqmtGcsCsvFile" for GCS source property path key "gcsSourcePath"
    Then Enter runtime argument value "gcsSkipHeaderTrue" for key "gcsSkipHeader"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "bqmtGcsCsvFileOutputSchema" for key "gcsOutputSchema"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "cmekBQ" for BigQuery cmek property key "cmekBQ" if BQ cmek is enabled
    Then Enter runtime argument value "bqTruncateTableTrue" for key "bqTruncateTable"
    Then Enter runtime argument value "bqmtUpdateTableSchemaTrue" for key "bqmtUpdateTableSchema"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "bqmtGcsCsvFile" for GCS source property path key "gcsSourcePath"
    Then Enter runtime argument value "gcsSkipHeaderTrue" for key "gcsSkipHeader"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "bqmtGcsCsvFileOutputSchema" for key "gcsOutputSchema"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "cmekBQ" for BigQuery cmek property key "cmekBQ" if BQ cmek is enabled
    Then Enter runtime argument value "bqTruncateTableTrue" for key "bqTruncateTable"
    Then Enter runtime argument value "bqmtUpdateTableSchemaTrue" for key "bqmtUpdateTableSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the cmek key "cmekBQ" of target BigQuery table if cmek is enabled
    Then Validate OUT record count is equal to IN record count
    Then Validate records transferred to target BQMT tables "bqmtCsvTargetTables"
