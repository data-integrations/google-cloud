Feature: CloudSQLPostGreSQL Sink Design Time

  @CLDPGSQL @TC-Mandatory-fields
  Scenario Outline:Verify CloudSQLPostGreSQL Sink properties validation errors for mandatory fields
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLPostGreSQL
    Then Open CloudSQLPostGreSQL Properties
    Then Enter the CloudSQLPostGreSQL Sink Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property |
      | referenceName  |
      | database  |
      | connectionName  |
      | tableName  |
      | jdbcPluginName  |

  @CLDPGSQL @TC-Invalid-TestData-for-DriverName_Field:
  Scenario: Verify Driver Name field validation error with invalid test data
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLPostGreSQL
    Then Open CloudSQLPostGreSQL Properties
    Then Enter Reference Name & Database Name with Test Data
    Then Enter Table Name "pSqlTableNameCS" and Connection Name "pSqlConnectionNameValid"
    Then Validate Connector properties
    Then Enter Driver Name with Invalid value
    Then Verify Driver Name field with Invalid value entered
    Then Close the CloudSQLPostGreSQL Properties

  @CLDPGSQL @TC-Invalid-TestData-for-ReferenceName&ConnectionName
  Scenario: User is able to Open and enter invalid test data for Reference Name & Connection Name
    Given Open DataFusion Project to configure pipeline
    When Target is CloudSQLPostGreSQL
    Then Enter Reference Name & Connection Name with Invalid Test Data in Sink
    Then Validate Connector properties
    Then Verify Reference Name Connection Name Fields with Invalid Test Data
    Then Enter Connection Name with private instance type
    Then Verify Connection Name with private instance type
    Then Close the CloudSQLPostGreSQL Properties





