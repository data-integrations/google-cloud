@BigQueryArgumentSetter
Feature: BigQuery Argument Setter - Validate BigQuery Argument Setter Plugin Error Scenarios

  Scenario: Verify BigQuery Argument Setter plugin properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Argument Setter" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Argument Setter"
    Then Click on the Validate button
    Then Verify mandatory property error for below listed properties:
      | dataset                     |
      | table                       |
      | argumentSelectionConditions |
      | argumentsColumns            |

  Scenario:Verify BigQuery Argument Setter plugin properties validation errors for blank data in Argument selection value field
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Conditions and Actions"
    When Select plugin: "BigQuery Argument Setter" from the plugins list as: "Conditions and Actions"
    When Navigate to the properties page of plugin: "BigQuery Argument Setter"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqSourceTable"
    Then Enter key value pairs for plugin property: "bqArgumentSetterConditionField" with values from json: "bqArgumentSetterBlankArgSelectionConditionValue"
    Then Enter the Arguments Columns field as list "bqArgumentSetterInvalidColumn"
    Then Override Service account details if set in environment variables
    Then Click on the Validate button
    Then Verify that the Plugin Property: "argumentSelectionConditions" is displaying an in-line error message: "errorMessageBlankBQArgumentColumnName"
