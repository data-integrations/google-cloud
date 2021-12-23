Feature: CloudSQLPostGreSQL Source Design Time and error validation

  @cloudSQLPostgreSQL
  Scenario Outline:Verify CloudSQLPostGreSQL Source properties validation errors for mandatory fields
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    Then Open cloudSQLPostgreSQL Properties
    Then Enter the CloudSQLPostGreSQL Source Properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property |
      | referenceName  |
      | database  |
      | connectionName  |
      | importQuery  |
      | jdbcPluginName  |

  @cloudSQLPostgreSQL
  Scenario:Verify Driver Name field validation error with invalid test data
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    Then Open cloudSQLPostgreSQL Properties
    Then Enter Reference Name & Database Name with Test Data
    Then Enter Connection Name and Import Query "cloudPsqlImportQuery"
    Then Validate Connector properties
    Then Enter Driver Name with Invalid value
    Then Verify Driver Name field with Invalid value entered
    Then Close the CloudSQLPostGreSQL Properties

  @cloudSQLPostgreSQL
  Scenario:Verify Reference Name & Connection Name field validation errors with invalid test data
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    Then Enter Reference Name & Connection Name with Invalid Test Data and import query "cloudPsqlImportQuery"
    Then Verify Reference Name Connection Name Fields with Invalid Test Data
    Then Enter Connection Name with private instance type
    Then Verify Connection Name with private instance type
    Then Close the CloudSQLPostGreSQL Properties
