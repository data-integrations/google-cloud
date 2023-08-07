@BigQuery_Sink
Feature: BigQuery sink - Validate BigQuery sink plugin error scenarios

  @BigQuery_Sink_Required
  Scenario Outline:Verify BigQuery Sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Sink is BigQuery
    Then Open BigQuery sink properties
    Then Enter the BigQuery properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property      |
      | dataset       |
      | table         |

  @BQ_SINK_TEST
  Scenario:Verify BigQuery Sink properties validation errors for incorrect value of chunk size
    Given Open Datafusion Project to configure pipeline
    When Sink is BigQuery
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery sink property table name
    Then Enter BigQuery sink property GCS upload request chunk size "bqInvalidChunkSize"
    Then Verify the BigQuery validation error message for invalid property "gcsChunkSize"

  @BQ_SINK_TEST
  Scenario:Verify BigQuery Sink properties validation errors for incorrect dataset
    Given Open Datafusion Project to configure pipeline
    When Sink is BigQuery
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "bqInvalidSinkDataset"
    Then Enter BigQuery sink property table name
    Then Verify the BigQuery validation error message for invalid property "dataset"

  @BQ_SINK_TEST
  Scenario:Verify BigQuery Sink properties validation errors for incorrect table
    Given Open Datafusion Project to configure pipeline
    When Sink is BigQuery
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery property table "bqInvalidSinkTable"
    Then Verify the BigQuery validation error message for invalid property "table"

  @BQ_SINK_TEST
  Scenario:Verify BigQuery Sink properties validation errors for incorrect value of temporary bucket name
    Given Open Datafusion Project to configure pipeline
    When Sink is BigQuery
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery sink property table name
    Then Enter BigQuery property temporary bucket name "bqInvalidTemporaryBucket"
    Then Verify the BigQuery validation error message for invalid property "bucket"
