@BigQuery_Sink
Feature: BigQuery sink - Validate BigQuery sink plugin error scenarios

  Scenario Outline:Verify BigQuery Sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Sink is BigQuery
    Then Open BigQuery sink properties
    Then Enter the BigQuery properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property       |
      | referenceName  |
      | dataset        |
      | table          |
