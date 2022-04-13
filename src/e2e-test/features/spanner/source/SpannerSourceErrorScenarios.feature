@Spanner_Source @SPANNER_TEST
Feature: Spanner Source - Verify Spanner source plugin error scenarios

  Scenario Outline:Verify Spanner source properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner
    Then Open Spanner source properties
    Then Enter the Spanner properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | instance        |
      | database        |
      | table           |

  Scenario Outline:Verify Spanner Source properties validation errors for incorrect values
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner
    Then Open Spanner source properties
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Enter the Spanner properties with incorrect property "<property>"
    Then Verify plugin properties validation fails with 1 error
    Examples:
      | property        |
      | instance        |
      | database        |
      | table           |
      | importQuery     |
