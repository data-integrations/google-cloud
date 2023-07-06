@Spanner_Source @SPANNER_TEST
Feature: Spanner Source - Verification of Spanner to BigQuery successful data transfer with macro arguments

  @BQ_SINK_TEST
  Scenario:Validate successful records transfer from Spanner to BigQuery with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner
    When Sink is BigQuery
    Then Open Spanner source properties
    Then Enter Spanner property reference name
    Then Enter Spanner property "projectId" as macro argument "spannerProjectId"
    Then Enter Spanner property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter Spanner property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter Spanner property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter Spanner property "instanceId" as macro argument "spannerInstanceId"
    Then Enter Spanner property "databaseName" as macro argument "spannerDatabaseName"
    Then Enter Spanner property "table" as macro argument "spannerTablename"
    Then Validate "Spanner" plugin properties
    Then Close the Spanner properties
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property "projectId" as macro argument "bqProjectId"
    Then Enter BigQuery property "datasetProjectId" as macro argument "bqDatasetProjectId"
    Then Enter BigQuery property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter BigQuery property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter BigQuery property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter BigQuery property "dataset" as macro argument "bqDataset"
    Then Enter BigQuery property "table" as macro argument "bqTargetTable"
    Then Enter BigQuery sink property "truncateTable" as macro argument "bqTruncateTable"
    Then Enter BigQuery sink property "updateTableSchema" as macro argument "bqUpdateTableSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "Spanner" and sink as "BigQuery" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "spannerProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value for Spanner Instance ID key "spannerInstanceId"
    Then Enter runtime argument value for Spanner Database Name key "spannerDatabaseName"
    Then Enter runtime argument value for Spanner Source Table Name key "spannerTablename"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery sink table name key "bqTargetTable"
    Then Enter runtime argument value "bqTruncateTableTrue" for key "bqTruncateTable"
    Then Enter runtime argument value "bqUpdateTableSchemaTrue" for key "bqUpdateTableSchema"
    Then Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "spannerProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value for Spanner Instance ID key "spannerInstanceId"
    Then Enter runtime argument value for Spanner Database Name key "spannerDatabaseName"
    Then Enter runtime argument value for Spanner Source Table Name key "spannerTablename"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery sink table name key "bqTargetTable"
    Then Enter runtime argument value "bqTruncateTableTrue" for key "bqTruncateTable"
    Then Enter runtime argument value "bqUpdateTableSchemaTrue" for key "bqUpdateTableSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target BigQuery table with record counts of spanner table
