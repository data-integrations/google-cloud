Feature: GCS to BQMT Design Time

  @BQMT @TC-Design-ReferenceName_Field:
  Scenario: TC-BQMT-DSGN-01:Verify Reference field is mandatory
    Given Open Datafusion Project
    When Target is "BigQueryMultiTable"
    Then Open BQMT Properties
    Then Validate Pipeline
    Then Verify Reference Field
    Then Close the BQMT Properties

  @TC-Design-DataSet_Field: @BQMT
  Scenario: TC-BQMT-DSGN-01:Verify Dataset field is mandatory
    Given Open Datafusion Project
    When Target is "BigQueryMultiTable"
    Then Open BQMT Properties
    Then Validate Pipeline
    Then Verify DataSet Field
    Then Close the BQMT Properties

  @TC-Invalid-TestData-for-ReferenceName&DataSet @BQMT
  Scenario: User is able to login and enter invalid test data for Reference Name & DataSet
    Given Open Datafusion Project
    When Target is "BigQueryMultiTable"
    Then Enter Reference Name & DataSet Fields with Invalid Test Data
    Then Verify Reference Name & DataSet Fields with Invalid Test Data
    Then Close the BQMT Properties

  @TC-Invalid-TestData-for-TemporaryBucket @BQMT
  Scenario: User is able to login and enter invalid test data for Temporary Bucket
    Given Open Datafusion Project
    When Target is "BigQueryMultiTable"
    Then Enter Temporary Bucket Field with Invalid Test Data and Verify the Error Message
    Then Close the BQMT Properties


  @TC-Add-Comments @BQMT
  Scenario: Add Comments for BigQuery Multi Table
    Given Open Datafusion Project to configure pipeline
    When Target is "BigQueryMultiTable"
    Then Add and Save Comments
    Then Edit Comments
    Then Delete Comments
