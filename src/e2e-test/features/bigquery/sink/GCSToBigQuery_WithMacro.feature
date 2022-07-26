@BigQuery_Sink
Feature: BigQuery sink - Verification of GCS to BigQuery successful data transfer with macro arguments

  @CMEK @GCS_CSV_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from GCS to BigQuery with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
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
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property "projectId" as macro argument "bqProjectId"
    Then Enter BigQuery property "datasetProjectId" as macro argument "bqDatasetProjectId"
    Then Enter GCS property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter GCS property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter GCS property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter BigQuery property "dataset" as macro argument "bqDataset"
    Then Enter BigQuery property "table" as macro argument "bqTargetTable"
    Then Enter BigQuery cmek property "encryptionKeyName" as macro argument "cmekBQ" if cmek is enabled
    Then Enter BigQuery sink property "truncateTable" as macro argument "bqTruncateTable"
    Then Enter BigQuery sink property "updateTableSchema" as macro argument "bqUpdateTableSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "gcsCsvFile" for GCS source property path key "gcsSourcePath"
    Then Enter runtime argument value "gcsSkipHeaderTrue" for key "gcsSkipHeader"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "gcsCSVFileOutputSchema" for key "gcsOutputSchema"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery sink table name key "bqTargetTable"
    Then Enter runtime argument value "cmekBQ" for BigQuery cmek property key "cmekBQ" if BQ cmek is enabled
    Then Enter runtime argument value "bqTruncateTableTrue" for key "bqTruncateTable"
    Then Enter runtime argument value "bqUpdateTableSchemaTrue" for key "bqUpdateTableSchema"
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
    Then Enter runtime argument value "gcsCsvFile" for GCS source property path key "gcsSourcePath"
    Then Enter runtime argument value "gcsSkipHeaderTrue" for key "gcsSkipHeader"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "gcsCSVFileOutputSchema" for key "gcsOutputSchema"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery sink table name key "bqTargetTable"
    Then Enter runtime argument value "cmekBQ" for BigQuery cmek property key "cmekBQ" if BQ cmek is enabled
    Then Enter runtime argument value "bqTruncateTableTrue" for key "bqTruncateTable"
    Then Enter runtime argument value "bqUpdateTableSchemaTrue" for key "bqUpdateTableSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Then Validate the cmek key "cmekBQ" of target BigQuery table if cmek is enabled

  @GCS_CSV_TEST @BQ_SINK_TEST @SERVICE_ACCOUNT_JSON_TEST
  Scenario:Validate successful records transfer from GCS to BigQuery with macro arguments - Service account type as Json
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Open GCS source properties
    Then Enter GCS property reference name
    Then Enter GCS property "projectId" as macro argument "gcsProjectId"
    Then Enter GCS property "serviceAccountType" as macro argument "gcsServiceAccountType"
    Then Enter GCS property "serviceAccountJSON" as macro argument "gcsServiceAccountJSON"
    Then Enter GCS property "path" as macro argument "gcsSourcePath"
    Then Enter GCS source property "skipHeader" as macro argument "gcsSkipHeader"
    Then Enter GCS property "format" as macro argument "gcsFormat"
    Then Enter GCS source property output schema "outputSchema" as macro argument "gcsOutputSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property "projectId" as macro argument "bqProjectId"
    Then Enter BigQuery property "datasetProjectId" as macro argument "bqDatasetProjectId"
    Then Enter BigQuery property "serviceAccountType" as macro argument "bqServiceAccountType"
    Then Enter BigQuery property "serviceAccountJSON" as macro argument "bqServiceAccountJSON"
    Then Enter BigQuery property "dataset" as macro argument "bqDataset"
    Then Enter BigQuery property "table" as macro argument "bqTargetTable"
    Then Enter BigQuery cmek property "encryptionKeyName" as macro argument "cmekBQ" if cmek is enabled
    Then Enter BigQuery sink property "truncateTable" as macro argument "bqTruncateTable"
    Then Enter BigQuery sink property "updateTableSchema" as macro argument "bqUpdateTableSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value "serviceAccountTypeJSON" for key "gcsServiceAccountType"
    Then Enter runtime argument value "serviceAccountJSON" for key "gcsServiceAccountJSON"
    Then Enter runtime argument value "gcsCsvFile" for GCS source property path key "gcsSourcePath"
    Then Enter runtime argument value "gcsSkipHeaderTrue" for key "gcsSkipHeader"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "gcsCSVFileOutputSchema" for key "gcsOutputSchema"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountTypeJSON" for key "bqServiceAccountType"
    Then Enter runtime argument value "serviceAccountJSON" for key "bqServiceAccountJSON"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery sink table name key "bqTargetTable"
    Then Enter runtime argument value "cmekBQ" for BigQuery cmek property key "cmekBQ" if BQ cmek is enabled
    Then Enter runtime argument value "bqTruncateTableTrue" for key "bqTruncateTable"
    Then Enter runtime argument value "bqUpdateTableSchemaTrue" for key "bqUpdateTableSchema"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value "serviceAccountTypeJSON" for key "gcsServiceAccountType"
    Then Enter runtime argument value "serviceAccountJSON" for key "gcsServiceAccountJSON"
    Then Enter runtime argument value "gcsCsvFile" for GCS source property path key "gcsSourcePath"
    Then Enter runtime argument value "gcsSkipHeaderTrue" for key "gcsSkipHeader"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "gcsCSVFileOutputSchema" for key "gcsOutputSchema"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountTypeJSON" for key "bqServiceAccountType"
    Then Enter runtime argument value "serviceAccountJSON" for key "bqServiceAccountJSON"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery sink table name key "bqTargetTable"
    Then Enter runtime argument value "cmekBQ" for BigQuery cmek property key "cmekBQ" if BQ cmek is enabled
    Then Enter runtime argument value "bqTruncateTableTrue" for key "bqTruncateTable"
    Then Enter runtime argument value "bqUpdateTableSchemaTrue" for key "bqUpdateTableSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Then Validate the cmek key "cmekBQ" of target BigQuery table if cmek is enabled
