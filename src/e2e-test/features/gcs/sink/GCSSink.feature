@GCS_Sink
Feature: GCS sink - Verification of GCS Sink plugin

  @CMEK @GCS_SINK_TEST @BQ_SOURCE_TEST
  Scenario: To verify data is getting transferred successfully from BigQuery to GCS
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Open BigQuery source properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS sink property path
    Then Select GCS property format "csv"
    Then Enter GCS property encryption key name "cmekGCS" if cmek is enabled
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
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
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
    Then Validate the cmek key "cmekGCS" of target GCS bucket if cmek is enabled

  @GCS_SINK_TEST @BQ_SOURCE_TEST @GCS_Sink_Required
  Scenario Outline: To verify data is getting transferred successfully from BigQuery to GCS for different formats
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS sink property path
    Then Select GCS property format "<FileFormat>"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
    Examples:
      | FileFormat |
      | csv        |
      | tsv        |
      | avro       |
      | json       |
      | orc        |
      | parquet    |

  @GCS_SINK_TEST @BQ_SOURCE_TEST
  Scenario Outline: To verify data is getting transferred successfully from BigQuery to GCS with combinations of contenttype
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS sink property path
    Then Select GCS property format "<FileFormat>"
    Then Select GCS sink property contentType "<contentType>"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket with file format "<FileFormat>"
    Examples:
      | FileFormat | contentType               |
      | avro       | application/avro          |
      | parquet    | application/octet-stream  |
      | orc        | application/octet-stream  |

  @GCS_SINK_TEST @BQ_SOURCE_TEST
  Scenario Outline: To verify data is getting transferred successfully from BigQuery to GCS with combinations of contenttype
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS sink property path
    Then Select GCS property format "<FileFormat>"
    Then Select GCS sink property contentType "<contentType>"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
    Examples:
      | FileFormat | contentType               |
      | csv        | application/csv           |
      | tsv        | text/tab-separated-values |
      | json       | application/json          |

  @GCS_SINK_TEST @BQ_SOURCE_TEST
  Scenario: To verify data is getting transferred successfully from BigQuery to GCS using output file prefix
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS sink property path
    Then Select GCS property format "csv"
    Then Enter GCS sink property output file prefix "gcsOutputFilePrefix"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket with fileName prefix "gcsOutputFilePrefix"

  @GCS_SINK_TEST @BQ_SOURCE_TEST
  Scenario: To verify data is getting transferred successfully from BigQuery to GCS using path suffix field
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS sink property path
    Then Select GCS property format "csv"
    Then Enter GCS sink property path suffix "gcsPathSuffix"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket with path suffix "gcsPathSuffix"

  @GCS_DATATYPE_TEST @GCS_SINK_TEST @GCS_Sink_Required
  Scenario: To verify data is getting transferred from GCS to GCS with supported DataTypes
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is GCS
    Then Connect source as "GCS" and sink as "GCS" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsDataTypeTestFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Click on the Get Schema button
    Then Change datatype of fields in output schema with : "gcsDatatypeChange"
    Then Validate "GCS" plugin properties
    Then Verify the Output Schema matches the Expected Schema: "gcsDataTypeTestFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open GCS sink properties
    Then Enter the GCS sink mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket

  @BQ_SOURCE_DATATYPE_TEST @GCS_SINK_TEST
  Scenario: To verify data is getting transferred successfully from BigQuery to GCS using json output format
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    Then Verify the Output Schema matches the Expected Schema: "bqSourceSchemaDatatype"
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS sink property path
    Then Select GCS property format "json"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
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
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
    Then Validate the values of records transferred to GCS bucket is equal to the values from source BigQuery table

  @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario: To verify data is getting transferred successfully from BigQuery to GCS using delimiter output format
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Click on the Get Schema button
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS sink property path
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "gcsDelimiter"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
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
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
