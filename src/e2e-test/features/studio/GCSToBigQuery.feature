@Studio
Feature: Studio-logs - BigQuery sink 2

  @GCS_CSV_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from GCS to BigQuery
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Open GCS source properties
    Then Enter the GCS source mandatory properties
    Then Validate plugin properties
    Then Wait for studio service error
    Then Navigate to System admin page
    Then Capture Pipeline studio service logs
    Then Capture App Fabric logs