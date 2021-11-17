Feature: Verify label,Reference Name,all the mandatory(*)field

  @fileconnector
  Scenario: Verify the Reference Name mandatory(*)field the properties (Blank Value)
    Given Open Datafusion Project to configure pipeline
    When Source is File bucket
    Then Enter the File Properties with "filepath" fileBucket and "csv" fileFormat without reference name
    Then Validate the Reference Name with blank value
    Then Close the File Properties

  @fileconnector
  Scenario: Verify the Path Value mandatory(*)field the properties (Blank Path Value)
    Given Open Datafusion Project to configure pipeline
    When Source is File bucket
    Then Enter the File Properties with "" File bucket and "csv" file format
    Then Validate the Path Name with blank Path value
    Then Close the File Properties

  @fileconnector
  Scenario: Verify Format mandatory(*)field the properties (Blank Format Value)
    Given Open Datafusion Project to configure pipeline
    When Source is File bucket
    Then Enter the File Properties with "filepath" File bucket and "" file format
    Then Validate the File Format with blank value
    Then Close the File Properties
