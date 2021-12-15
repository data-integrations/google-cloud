Feature: CloudSQLPostGreSQL Source Design Time

  @CLDPGSQL @TC-Mandatory-fields
  Scenario Outline:Verify CloudSQLPostGreSQL Source properties validation errors for mandatory fields
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    Then Open CloudSQLPostGreSQL Properties
    Then Enter the CloudSQLPostGreSQL Source Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property |
      | referenceName  |
      | database  |
      | connectionName  |
      | importQuery  |
      | jdbcPluginName  |

  @CLDPGSQL @TC-Invalid-TestData-for-DriverName_Field:
  Scenario: TC-CLDMYSQL-DSGN-03:Verify Driver Name field validation error with invalid test data
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    Then Open CloudSQLPostGreSQL Properties
    Then Enter Reference Name & Database Name with Test Data
    Then Enter Connection Name and Import Query "pSqlImportQuery"
    Then Validate Connector properties
    Then Enter Driver Name with Invalid value
    Then Verify Driver Name field with Invalid value entered
    Then Close the CloudSQLPostGreSQL Properties

  @CLDPGSQL @TC-Invalid-TestData-for-ReferenceName&ConnectionName
  Scenario: TC-CLDMYSQL-DSGN-04:Verify Reference Name & Connection Name field validation errors with invalid test data
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    Then Enter Reference Name & Connection Name with Invalid Test Data and import query "pSqlImportQuery"
    Then Verify Reference Name Connection Name Fields with Invalid Test Data
    Then Enter Connection Name with private instance type
    Then Verify Connection Name with private instance type
    Then Close the CloudSQLPostGreSQL Properties
