@BQMT_Sink
Feature: BQMT-Sink - Verify BQMT sink plugin error Scenarios

  Scenario Outline: Verify BQMT sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Sink is BiqQueryMultiTable
    Then Open the BiqQueryMultiTable sink properties
    Then Enter the BiqQueryMultiTable sink properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property      |
      | referenceName |
      | dataset       |

  Scenario:Verify BQMT Sink properties validation errors for incorrect value of chunk size
    Given Open Datafusion Project to configure pipeline
    When Sink is BiqQueryMultiTable
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Enter BiqQueryMultiTable sink property GCS upload request chunk size "bqmtInvalidChunkSize"
    Then Verify the BiqQueryMultiTable sink validation error message for invalid property "gcsChunkSize"

  Scenario:Verify BQMT Sink properties validation errors for incorrect dataset
    Given Open Datafusion Project to configure pipeline
    When Sink is BiqQueryMultiTable
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "bqmtInvalidSinkDataset"
    Then Verify the BiqQueryMultiTable sink validation error message for invalid property "dataset"

  Scenario:Verify BQMT Sink properties validation errors for incorrect reference name
    Given Open Datafusion Project to configure pipeline
    When Sink is BiqQueryMultiTable
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name "bqmtInvalidSinkReferenceName"
    Then Verify the BiqQueryMultiTable sink validation error message for invalid property "referenceName"

  Scenario:Verify BQMT Sink properties validation errors for incorrect value of temporary bucket name
    Given Open Datafusion Project to configure pipeline
    When Sink is BiqQueryMultiTable
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Enter BiqQueryMultiTable sink property temporary bucket name "bqmtInvalidTemporaryBucket"
    Then Verify the BiqQueryMultiTable sink validation error message for invalid property "bucket"
