Feature: Runtime

  @TC-Success4 @GCS
  Scenario: User is able to Login and data is getting transferred from GCS to BigQuery
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


  @TC-LargeData @GCS
  Scenario: User is able to Login and data is getting transferred from GCS to BigQuery with LargeData
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "heavyData" and format "gcsCSVFileFormat"
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed

  @TC-csv @GCS
  Scenario: Verify csv file formats
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

  @TC-tsv @GCS
  Scenario: Verify tsv file formats
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsTsvbucket" and format "gcsTSVFileFormat"
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed

  @TC-text @GCS
  Scenario: Verify text file formats
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsTextbucket" and format "gcsTextFileFormat"
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for the table "gcsBqTableName"
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed