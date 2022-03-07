@Studio
Feature: Studio-logs - BigQuery source 1

  @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to GCS
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Enter BiqQuery property encryption key name "cmekBQ" if cmek is enabled
    Then Validate plugin properties
    Then Wait for studio service error
    Then Capture Pipeline studio service logs
