@Spanner_Sink @SPANNER_TEST
Feature: Spanner Sink - Verification of BigQuery to Spanner successful data transfer with macro arguments

  @BQ_SOURCE_TEST @SPANNER_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to Spanner with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is Spanner
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
    Then Open Spanner sink properties
    Then Enter Spanner property reference name
    Then Enter Spanner property "projectId" as macro argument "spannerProjectId"
    Then Enter Spanner property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter Spanner property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter Spanner property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter Spanner property "instanceId" as macro argument "spannerInstanceId"
    Then Enter Spanner property "databaseName" as macro argument "spannerDatabaseName"
    Then Enter Spanner property "table" as macro argument "spannerTablename"
    Then Enter Spanner property "primaryKey" as macro argument "spannerSinkPrimaryKey"
    Then Enter Spanner cmek property "encryptionKeyName" as macro argument "cmekSpanner" if cmek is enabled
    Then Validate "Spanner" plugin properties
    Then Close the Spanner properties
    Then Connect source as "BigQuery" and sink as "Spanner" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery source table name key "bqSourceTable"
    Then Enter runtime argument value "projectId" for key "spannerProjectId"
    Then Enter runtime argument value for Spanner Instance ID key "spannerInstanceId"
    Then Enter runtime argument value for Spanner Database Name key "spannerDatabaseName"
    Then Enter runtime argument value for Spanner Sink Table Name key "spannerTablename"
    Then Enter runtime argument value "spannerSinkPrimaryKeyBQ" for key "spannerSinkPrimaryKey"
    Then Enter runtime argument value "cmekSpanner" for Spanner cmek property key "cmekSpanner" if Spanner cmek is enabled
    Then Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "success"
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery source table name key "bqSourceTable"
    Then Enter runtime argument value "projectId" for key "spannerProjectId"
    Then Enter runtime argument value for Spanner Instance ID key "spannerInstanceId"
    Then Enter runtime argument value for Spanner Database Name key "spannerDatabaseName"
    Then Enter runtime argument value for Spanner Sink Table Name key "spannerTablename"
    Then Enter runtime argument value "spannerSinkPrimaryKeyBQ" for key "spannerSinkPrimaryKey"
    Then Enter runtime argument value "cmekSpanner" for Spanner cmek property key "cmekSpanner" if Spanner cmek is enabled
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target spanner table with record counts of BigQuery table
