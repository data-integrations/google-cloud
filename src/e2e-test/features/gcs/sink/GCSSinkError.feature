@GCS_Sink
Feature: GCS sink - Verify GCS Sink plugin error scenarios

  Scenario Outline:Verify GCS Sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Sink is GCS
    Then Open GCS sink properties
    Then Enter the GCS properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | path            |
      | format          |
