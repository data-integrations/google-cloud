@Studio
Feature: Studio-logs - BigQuery sink 1

  Scenario Outline:Verify BigQuery Sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Sink is BigQuery
    Then Open BigQuery sink properties
    Then Enter the BigQuery properties with blank property "<property>"
    Then Validate plugin properties
    Then Wait for studio service error
    Then Navigate to System admin page
    Then Capture Pipeline studio service logs
    Then Capture App Fabric logs
    Examples:
      | property       |
      | referenceName  |
      | dataset        |
      | table          |
