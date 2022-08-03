@BQMT_Sink
Feature: BQMT-Sink - Verify GCS to BQMT sink plugin data transfer scenarios

  @CMEK @GCS_CSV_BQMT_TEST @BQ_SINK_TEST_CSV_BQMT
  Scenario: Verify data is getting transferred from GCS to BQMT sink for csv format
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BiqQueryMultiTable
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "bqmtGcsCsvFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "bqmtGcsCsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Toggle BiqQueryMultiTable sink property truncateTable to "true"
    Then Toggle BiqQueryMultiTable sink property allow flexible schema to "true"
    Then Select BiqQueryMultiTable sink property update table schema as "true"
    Then Enter BiqQueryMultiTable sink property encryption key name "cmekBQ" if cmek is enabled
    Then Validate "BigQueryMultiTable" plugin properties
    Then Close the BiqQueryMultiTable properties
    Then Connect source as "GCS" and sink as "BigQueryMultiTable" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Validate records transferred to target BQMT tables "bqmtCsvTargetTables"
    Then Validate the cmek key "cmekBQ" of target BQMT tables "bqmtCsvTargetTables" if cmek is enabled

  @GCS_DELIMITED_TEST @BQ_SINK_TEST_DELIMITED_BQMT
  Scenario: Verify data is getting transferred from GCS to BQMT sink with Delimiter
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BiqQueryMultiTable
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsDelimitedFile"
    Then Select GCS property format "delimited"
    Then Enter GCS property delimiter "gcsDelimiter"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsDelimitedFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Toggle BiqQueryMultiTable sink property truncateTable to "true"
    Then Toggle BiqQueryMultiTable sink property allow flexible schema to "true"
    Then Select BiqQueryMultiTable sink property update table schema as "true"
    Then Enter BiqQueryMultiTable sink property encryption key name "cmekBQ" if cmek is enabled
    Then Validate "BigQueryMultiTable" plugin properties
    Then Close the BiqQueryMultiTable properties
    Then Connect source as "GCS" and sink as "BigQueryMultiTable" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Validate records transferred to target BQMT tables "bqmtDelimitedTargetTables"
    Then Validate the cmek key "cmekBQ" of target BQMT tables "bqmtDelimitedTargetTables" if cmek is enabled

  @GCS_TXT_BQMT_TEST @BQ_SINK_TEST_TXT_BQMT @PLUGIN-1179
  Scenario: Verify data is getting transferred from GCS to BQMT sink with text format
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BiqQueryMultiTable
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "bqmtGcsTextFile"
    Then Select GCS property format "text"
    Then Enter GCS property delimiter "bqmtGcsDelimiter"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "bqmtGcsTextFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Toggle BiqQueryMultiTable sink property truncateTable to "true"
    Then Toggle BiqQueryMultiTable sink property allow flexible schema to "true"
    Then Select BiqQueryMultiTable sink property update table schema as "true"
    Then Enter BiqQueryMultiTable sink property encryption key name "cmekBQ" if cmek is enabled
    Then Validate "BigQueryMultiTable" plugin properties
    Then Close the BiqQueryMultiTable properties
    Then Connect source as "GCS" and sink as "BigQueryMultiTable" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Validate records transferred to target BQMT tables "bqmtTextTargetTables"
    Then Validate the cmek key "cmekBQ" of target BQMT tables "bqmtTextTargetTables" if cmek is enabled

  @GCS_TSV_BQMT_TEST @BQ_SINK_TEST_TSV_BQMT
  Scenario: Verify data is getting transferred from GCS to BQMT sink with TSV format
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BiqQueryMultiTable
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "bqmtGcsTsvFile"
    Then Select GCS property format "tsv"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "bqmtTsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Toggle BiqQueryMultiTable sink property truncateTable to "true"
    Then Toggle BiqQueryMultiTable sink property allow flexible schema to "true"
    Then Select BiqQueryMultiTable sink property update table schema as "true"
    Then Enter BiqQueryMultiTable sink property encryption key name "cmekBQ" if cmek is enabled
    Then Validate "BigQueryMultiTable" plugin properties
    Then Close the BiqQueryMultiTable properties
    Then Connect source as "GCS" and sink as "BigQueryMultiTable" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Validate records transferred to target BQMT tables "bqmtTsvTargetTables"
    Then Validate the cmek key "cmekBQ" of target BQMT tables "bqmtTsvTargetTables" if cmek is enabled

  @GCS_TSV_BQMT_TEST @BQ_SINK_TEST_SPLIT_BQMT
  Scenario: Verify data is getting transferred from GCS to BQMT sink with split field
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BiqQueryMultiTable
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "bqmtGcsTsvFile"
    Then Select GCS property format "tsv"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "bqmtTsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Toggle BiqQueryMultiTable sink property truncateTable to "true"
    Then Toggle BiqQueryMultiTable sink property allow flexible schema to "true"
    Then Enter BiqQueryMultiTable sink property split field "bqmtSplitField"
    Then Select BiqQueryMultiTable sink property update table schema as "true"
    Then Enter BiqQueryMultiTable sink property encryption key name "cmekBQ" if cmek is enabled
    Then Validate "BigQueryMultiTable" plugin properties
    Then Close the BiqQueryMultiTable properties
    Then Connect source as "GCS" and sink as "BigQueryMultiTable" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Validate records transferred to target BQMT tables "bqmtSplitFieldTargetTables"
    Then Validate the cmek key "cmekBQ" of target BQMT tables "bqmtSplitFieldTargetTables" if cmek is enabled

  @GCS_CSV_BQMT_TEST @BQ_SINK_TEST_CSV_BQMT
  Scenario: Verify data is getting transferred from GCS to BQMT with truncate and update table schema false
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BiqQueryMultiTable
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "bqmtGcsCsvFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "bqmtGcsCsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Toggle BiqQueryMultiTable sink property allow flexible schema to "true"
    Then Select BiqQueryMultiTable sink property update table schema as "false"
    Then Enter BiqQueryMultiTable sink property encryption key name "cmekBQ" if cmek is enabled
    Then Validate "BigQueryMultiTable" plugin properties
    Then Close the BiqQueryMultiTable properties
    Then Connect source as "GCS" and sink as "BigQueryMultiTable" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Validate records transferred to target BQMT tables "bqmtCsvTargetTables"
    Then Validate the cmek key "cmekBQ" of target BQMT tables "bqmtCsvTargetTables" if cmek is enabled

  @GCS_BLOB_BQMT_TEST @BQ_SINK_TEST_BLOB_BQMT @PLUGIN-1179
  Scenario: Verify data is getting transferred from GCS to BQMT sink with Blob format
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BiqQueryMultiTable
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "bqmtGcsBlobFile"
    Then Select GCS property format "blob"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "bqmtGcsBlobFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Toggle BiqQueryMultiTable sink property truncateTable to "true"
    Then Toggle BiqQueryMultiTable sink property allow flexible schema to "true"
    Then Select BiqQueryMultiTable sink property update table schema as "true"
    Then Enter BiqQueryMultiTable sink property encryption key name "cmekBQ" if cmek is enabled
    Then Validate "BigQueryMultiTable" plugin properties
    Then Close the BiqQueryMultiTable properties
    Then Connect source as "GCS" and sink as "BigQueryMultiTable" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Validate records transferred to target BQMT tables "bqmtBlobTargetTables"
    Then Validate the cmek key "cmekBQ" of target BQMT tables "bqmtBlobTargetTables" if cmek is enabled
