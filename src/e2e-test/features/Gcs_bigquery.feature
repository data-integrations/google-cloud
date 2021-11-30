Feature: Design-time

  @TC-plugin-857 @GCS
  Scenario: Verify automatic datatype detection of Timestamp in GCS
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "timestamp_test" and format "gcsCSVFileFormat"
    Then verify the datatype

  @TC-plugin-855 @GCS
  Scenario: Verify File encoding types
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsCsvbucket" , format "gcsCSVFileFormat" and fileEncoding 32
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed

 @TC-Design_time_mandatoryfields @GCS
  Scenario: To verify mandatory fields in design time from GCS to BigQuery
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsCsvbucket" and format "gcsCSVFileFormat"
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed


  @TC-DesigntimeALLfields @GCS
  Scenario: To verify advance fields in design time from GCS to BigQuery
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "output_fieldtest" and format "gcsCSVFileFormat" by entering all fields
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Validate successMessage is displayed

  @TC-OverrideDatatype @GCS
  Scenario: To verify OverrideDatatype field
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "output_fieldtest" and format "gcsCSVFileFormat" using OverrideDatatype
    Then verify the schema in output
