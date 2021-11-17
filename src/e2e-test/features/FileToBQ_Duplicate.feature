Feature: Verify the user is able to create pipline with All formats in CDAP

  @fileconnector
  Scenario: Verify csv file formats for Duplicate Pipeline
    Given Open Datafusion Project to configure pipeline
    When Source is File bucket
    When Target is BigQuery
    Then Link Source-File and Sink-BigQuery to establish connection
    Then Enter the File Properties with "filepath" File bucket and "csv" file format
    Then Close the File Properties
    Then Enter the BigQuery Properties for table "demoCheck1" with JSON
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Create Duplicate pileline

  @fileconnector
  Scenario: Verify parquet formats for Duplicate Pipeline
    Given Open Datafusion Project to configure pipeline
    When Source is File bucket
    When Target is BigQuery
    Then Link Source-File and Sink-BigQuery to establish connection
    Then Enter the File Properties with "filepathParquet" File bucket and "parquet" file format
    Then Close the File Properties
    Then Enter the BigQuery Properties for table "demoCheck1" with JSON
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Create Duplicate pileline

  @fileconnector
  Scenario: Verify avro formats for Duplicate Pipeline
    Given Open Datafusion Project to configure pipeline
    When Source is File bucket
    When Target is BigQuery
    Then Link Source-File and Sink-BigQuery to establish connection
    Then Enter the File Properties with "fileAvro" File bucket and "avro" file format
    Then Close the File Properties
    Then Enter the BigQuery Properties for table "demoCheck1" with JSON
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Create Duplicate pileline
