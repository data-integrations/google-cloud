Feature: Verify Errors

  @TC-Error-1
  Scenario: Verify Error validation in reference field
  Given Open Datafusion Project to configure pipeline
  When Source is GCS bucket
  When Target is BigQuery
  Then Link Source and Sink to establish connection
  Then Enter the GCS Properties with GCS bucket "gcsCsvbucket" and format "gcsCSVFileFormat" by entering blank referenceName
  Then Verify reference name is mandatory

  @TC-Error-2
  Scenario: Verify Error validation in path field
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with format "gcsCSVFileFormat" by entering blank path
    Then Verify path is mandatory