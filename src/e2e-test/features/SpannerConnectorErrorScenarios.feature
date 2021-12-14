Feature: Verify Spanner plugin error scenarios

  @Spanner
  Scenario Outline:Verify Spanner Source properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner Connector
    Then Open Spanner connector properties
    Then Enter the Spanner connector Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | instance        |
      | database        |
      | table           |

  @Spanner
  Scenario Outline:Verify Spanner Source properties validation errors for incorrect values
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner Connector
    Then Open Spanner connector properties
    Then Enter the Spanner connector Properties with incorrect property "<property>"
    Then Verify plugin validation fails with error
    Examples:
      | property        |
      | instance        |
      | database        |
      | table           |
      | importQuery     |
