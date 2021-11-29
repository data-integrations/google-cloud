Feature: Verify Errors

  @TC-Error-1
  Scenario: Verify Error validation in reference field
    When Source is BigQuery bucket
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "Employeedata" amd dataset "test_automation" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gs://cdf-athena" and format "avro"
    Then Verify reference name validation

  @TC-Error-2
  Scenario: Verify Error validation in path field
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "Employeedata" amd dataset "test_automation" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gs://cdf-athena" and format "avro"
    Then Verify path validation