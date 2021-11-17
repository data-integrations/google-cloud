Feature: Verify Add,Edit,Delete Comments for File Connector in CDAP

  @fileconnector
  Scenario: Verify Add Comments for File Connector in CDAP
    Given Open Datafusion Project to configure pipeline
    When Source is File bucket
    Then Open the File Properties and add comments
    Then Close the File Properties
    Then Veriy the Comments over File Propertie window

  @fileconnector
  Scenario: Verify Edit Comments for File Connector in CDAP
    Given Open Datafusion Project to configure pipeline
    When Source is File bucket
    Then Open the File Properties and add comments
    Then Close the File Properties
    Then Veriy the Edit Comments over File Propertie window

  @fileconnector
  Scenario: Verify Delete Comments for File Connector in CDAP
    Given Open Datafusion Project to configure pipeline
    When Source is File bucket
    Then Open the File Properties and add comments
    Then Close the File Properties
    Then Veriy the Delete Comments over File Propertie window
    Then Delete the Comment window
