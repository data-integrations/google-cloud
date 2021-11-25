Feature: Runtime

  @TC-Success4
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
    Then Verify the pipeline status in each case
    Then Open Logs
    Then validate successMessage is displayed


  @TC-Failed
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
    Then Verify the pipeline status in each case
    Then Open Logs
#    Then validate failedMessage is displayed

  @TC-csv
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
    Then Verify the pipeline status in each case
    Then Open Logs
    Then validate successMessage is displayed

  @TC-tsv
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
    Then Verify the pipeline status in each case
    Then Open Logs
    Then validate successMessage is displayed

  @TC-text
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
    Then Verify the pipeline status in each case
    Then Open Logs
    Then validate successMessage is displayed