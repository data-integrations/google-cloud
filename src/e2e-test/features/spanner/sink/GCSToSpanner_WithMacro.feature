@Spanner_Sink @SPANNER_TEST
Feature: Spanner Sink - Verification of GCS to Spanner successful data transfer with macro arguments

  @GCS_CSV_TEST @SPANNER_SINK_TEST
  Scenario:Validate successful records transfer from GCS to Spanner with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is Spanner
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
    Then Connect source as "GCS" and sink as "Spanner" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "gcsCsvFile" for GCS source property path key "gcsSourcePath"
    Then Enter runtime argument value "gcsSkipHeaderTrue" for key "gcsSkipHeader"
    Then Enter runtime argument value "csvFormat" for key "gcsFormat"
    Then Enter runtime argument value "gcsCSVFileOutputSchema" for key "gcsOutputSchema"
    Then Enter runtime argument value "projectId" for key "spannerProjectId"
    Then Enter runtime argument value for Spanner Instance ID key "spannerInstanceId"
    Then Enter runtime argument value for Spanner Database Name key "spannerDatabaseName"
    Then Enter runtime argument value for Spanner Sink Table Name key "spannerTablename"
    Then Enter runtime argument value "spannerSinkPrimaryKeyGCS" for key "spannerSinkPrimaryKey"
    Then Enter runtime argument value "cmekSpanner" for Spanner cmek property key "cmekSpanner" if Spanner cmek is enabled
    Then Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "success"
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
    Then Enter runtime argument value "projectId" for key "spannerProjectId"
    Then Enter runtime argument value for Spanner Instance ID key "spannerInstanceId"
    Then Enter runtime argument value for Spanner Database Name key "spannerDatabaseName"
    Then Enter runtime argument value for Spanner Sink Table Name key "spannerTablename"
    Then Enter runtime argument value "spannerSinkPrimaryKeyGCS" for key "spannerSinkPrimaryKey"
    Then Enter runtime argument value "cmekSpanner" for Spanner cmek property key "cmekSpanner" if Spanner cmek is enabled
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
