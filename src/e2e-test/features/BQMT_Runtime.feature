Feature: GCS to BQMT RunTime

  @BQMT @TC-Single-GCS-to-BigQueryMultiTable-csvformat
  Scenario: User is able to Login and confirm csv format data is getting transferred from GCS to BigQueryMultiTable
    Given Open Datafusion Project
    When Source selected is GCS bucket
    When Target is "BigQueryMultiTable"
    Then Link GCS to "BigQueryMultiTable" to establish connection
    Then Enter the source GCS Properties with format "@TC-File_Format1" GCS bucket "@TC-BQMT-File1"
    Then Verify the get schema status
    Then Validate the Schema
    Then Verify the Connector status
    Then Close the Properties of GCS
    Then Enter the BQMT Properties
    Then Verify the Connector status
    Then Close the BQMT Properties
    Then Save and Deploy Pipeline of GCS to BQMT
    Then Run the Pipeline in Runtime  to transfer record
    Then Wait till pipeline run
    Then Open and capture Logs
    Then Verify the pipeline status is "Succeeded" for the pipeline
    Then Get Count of no of records transferred to BigQuery "BigQueryMultiTable-csvformat_table1" "BigQueryMultiTable-csvformat_table2" "BigQueryMultiTable-csvformat_table3"
    Then Delete the BQMT table "BigQueryMultiTable-csvformat_table1"
    Then Delete the BQMT table "BigQueryMultiTable-csvformat_table2"
    Then Delete the BQMT table "BigQueryMultiTable-csvformat_table3"
