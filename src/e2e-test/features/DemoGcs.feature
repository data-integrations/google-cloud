Feature: Demo1

  @TC-Demo-1
  Scenario: User is able to Login and confirm data is getting transferred from GCS to BigQuery
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with "@TC-Demo-1_GCS" GCS bucket
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for table "tableDemo"
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed
    Then Get Count of no of records transferred to BigQuery in "tableDemo"
    Then Delete the table "tableDemo"

  @TC-Demo-1
  Scenario: User is able to confirm data is transferred from GCS to BQ table which is created using the provided CMEK.
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with "@TC-Demo-1_GCS" GCS bucket
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for table "tableDemo"
    Then Close the BigQuery Properties
    Then Enter CmekKey property in BigQuery Sink "bq-cmek-test"
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed
    Then Get Count of no of records transferred to BigQuery in "tableDemo"
    Then Verify the CmekKey "bq-cmek-test" used to create table "tableDemo"
    Then Delete the table "tableDemo"