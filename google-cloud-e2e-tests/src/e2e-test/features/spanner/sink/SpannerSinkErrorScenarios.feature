@Spanner_Sink
Feature: Spanner Sink - Verify Spanner sink plugin error scenarios

  Scenario Outline:Verify Spanner sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Sink is Spanner
    Then Open Spanner sink properties
    Then Enter the Spanner properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | instance        |
      | database        |
      | table           |
