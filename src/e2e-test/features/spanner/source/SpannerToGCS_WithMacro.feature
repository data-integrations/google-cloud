@Spanner_Source @SPANNER_TEST
Feature: Spanner Source - Verification of Spanner to GCS successful data transfer with macro arguments

  @GCS_SINK_TEST
  Scenario:Validate successful records transfer from Spanner to GCS with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner
    When Sink is GCS
    Then Open Spanner source properties
    Then Enter Spanner property reference name
    Then Enter Spanner property "projectId" as macro argument "spannerProjectId"
    Then Enter Spanner property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter Spanner property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter Spanner property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter Spanner property "instanceId" as macro argument "spannerInstanceId"
    Then Enter Spanner property "databaseName" as macro argument "spannerDatabaseName"
    Then Enter BigQuery property "table" as macro argument "spannerTablename"
    Then Validate "Spanner" plugin properties
    Then Close the Spanner properties
    Then Open GCS sink properties
    Then Enter GCS property reference name
    Then Enter GCS property "projectId" as macro argument "gcsProjectId"
    Then Enter GCS property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter GCS property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter GCS property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter GCS property "path" as macro argument "gcsSinkPath"
    Then Enter GCS sink property "pathSuffix" as macro argument "gcsPathSuffix"
    Then Enter GCS property "format" as macro argument "gcsFormat"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Connect source as "Spanner" and sink as "GCS" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "spannerProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value for Spanner Instance ID key "spannerInstanceId"
    Then Enter runtime argument value for Spanner Database Name key "spannerDatabaseName"
    Then Enter runtime argument value for Spanner Source Table Name key "spannerTablename"
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value for GCS sink property path key "gcsSinkPath"
    Then Enter runtime argument value "gcsPathDateSuffix" for key "gcsPathSuffix"
    Then Enter runtime argument value "avroFormat" for key "gcsFormat"
    Then Run the preview of pipeline with runtime arguments
    Then Verify the preview of pipeline is "success"
    Then Click on preview data for GCS sink
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "projectId" for key "spannerProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value for Spanner Instance ID key "spannerInstanceId"
    Then Enter runtime argument value for Spanner Database Name key "spannerDatabaseName"
    Then Enter runtime argument value for Spanner Source Table Name key "spannerTablename"
    Then Enter runtime argument value "projectId" for key "gcsProjectId"
    Then Enter runtime argument value for GCS sink property path key "gcsSinkPath"
    Then Enter runtime argument value "gcsPathDateSuffix" for key "gcsPathSuffix"
    Then Enter runtime argument value "avroFormat" for key "gcsFormat"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
