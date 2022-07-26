@MultiFile
Feature: GCSMultiFile Sink - Verification of successful data transfer using GCSMultiFile as sink

  @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario: To verify data is getting transferred from BigQuery to GcsMultiFile
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCSMultiFile
    Then Connect source as "BigQuery" and sink as "GCSMultiFiles" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property projectId "projectId"
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property path
    Then Select GCSMultiFile property format "csv"
    Then Toggle GCSMultiFile property allow flexible schemas to true
    Then Validate "GCSMultiFiles" plugin properties
    Then Close GCSMultiFile Properties
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

  @GCS_CSV_TEST @GCS_SINK_TEST
  Scenario: To verify data is getting transferred from GCS to GcsMultiFile
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is GCSMultiFile
    Then Connect source as "GCS" and sink as "GCSMultiFiles" to establish connection
    Then Open GCS source properties
    Then Enter the GCS source mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property projectId "projectId"
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property path
    Then Select GCSMultiFile property format "csv"
    Then Toggle GCSMultiFile property allow flexible schemas to true
    Then Validate "GCSMultiFiles" plugin properties
    Then Close GCSMultiFile Properties
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

  @BQ_MULTIPLE_SOURCE @GCS_CSV_MULTIPLE_SOURCE @GCS_SINK_TEST
  Scenario: To verify data is getting transferred from MultipleSource to GcsMultiFile
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is GCSMultiFile
    Then Connect source as "GCS" and sink as "GCSMultiFiles" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "multiFileGCSBucket"
    Then Toggle GCS source property skip header to true
    Then Select GCS property format "csv"
    Then Click on the Get Schema button
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    When Expand Plugin group in the LHS plugins list: "Source"
    Then Source is BigQuery
    Then Connect source as "BigQuery" and sink as "GCSMultiFiles" to establish connection
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property projectId "projectId"
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property path
    Then Select GCSMultiFile property format "csv"
    Then Toggle GCSMultiFile property allow flexible schemas to true
    Then Validate "GCSMultiFiles" plugin properties
    Then Close GCSMultiFile Properties
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

  @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario Outline: To verify data is getting transferred from BigQuery to GcsMultiFile with different file Formats
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCSMultiFile
    Then Connect source as "BigQuery" and sink as "GCSMultiFiles" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property projectId "projectId"
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property path
    Then Select GCSMultiFile property format "<FormatType>"
    Then Toggle GCSMultiFile property allow flexible schemas to true
    Then Validate "GCSMultiFiles" plugin properties
    Then Close GCSMultiFile Properties
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
    Examples:
      | FormatType |
      | csv        |
      | avro       |
      | parquet    |
      | json       |
      | orc        |
      | tsv        |

  @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario: To verify data is getting transferred from BigQuery to GcsMultifile with Delimited Format
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCSMultiFile
    Then Connect source as "BigQuery" and sink as "GCSMultiFiles" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property projectId "projectId"
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property path
    Then Select GCSMultiFile property format "delimited"
    Then Enter GCSMultiFile property delimiter "multiFileDelimiter"
    Then Toggle GCSMultiFile property allow flexible schemas to true
    Then Validate "GCSMultiFiles" plugin properties
    Then Close GCSMultiFile Properties
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

  @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario Outline: To verify data is getting transferred from BigQuery to GcsMultiFile using Codec format
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCSMultiFile
    Then Connect source as "BigQuery" and sink as "GCSMultiFiles" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property projectId "projectId"
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property path
    Then Select GCSMultiFile property format "<FormatType>"
    Then Select GCSMultiFile property CodecType "<CodecType>"
    Then Toggle GCSMultiFile property allow flexible schemas to true
    Then Validate "GCSMultiFiles" plugin properties
    Then Close GCSMultiFile Properties
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
    Examples:
      | FormatType | CodecType                 |
      | avro       | multiFileSnappyCodecType  |
      | avro       | multiFileDeflateCodecType |
      | parquet    | multiFileSnappyCodecType  |
      | parquet    | multiFileGZipCodecType    |

  @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario Outline: To verify data is getting transferred from BigQuery to GcsMultiFile using ContentType and format combinations
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCSMultiFile
    Then Connect source as "BigQuery" and sink as "GCSMultiFiles" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property projectId "projectId"
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property path
    Then Select GCSMultiFile property format "<FormatType>"
    Then Select GCSMultiFile property ContentType "<ContentType>"
    Then Toggle GCSMultiFile property allow flexible schemas to true
    Then Validate "GCSMultiFiles" plugin properties
    Then Close GCSMultiFile Properties
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
    Then Verify data is transferred to target GCS bucket with file format "<FormatType>"
    Examples:
      | FormatType | ContentType              |
      | avro       | application/avro         |
      | parquet    | application/octet-stream |
      | orc        | application/octet-stream |
    @PLUGIN-808
    Examples:
      | FormatType | ContentType               |
      | csv        | application/csv           |
      | tsv        | text/tab-separated-values |
      | json       | application/json          |

  @GCS_CSV_SPLIT_FIELD @GCS_SINK_TEST
  Scenario: To verify data is getting transferred from BigQuery to GcsMultiFile using Split field
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is GCSMultiFile
    Then Connect source as "GCS" and sink as "GCSMultiFiles" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "multiFileSplitFieldBucket"
    Then Toggle GCS source property skip header to true
    Then Select GCS property format "csv"
    Then Click on the Get Schema button
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property projectId "projectId"
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property path
    Then Select GCSMultiFile property format "csv"
    Then Toggle GCSMultiFile property allow flexible schemas to true
    Then Validate "GCSMultiFiles" plugin properties
    Then Close GCSMultiFile Properties
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
    Then Verify folders with split field are created in GCS bucket for tables "multifileSplitTables"
    Then Verify data is transferred to target GCS bucket

  @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario: To verify data is getting transferred successfully from BigQuery to GCS using path suffix field
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCSMultiFile
    Then Connect source as "BigQuery" and sink as "GCSMultiFiles" to establish connection
    Then Open BigQuery source properties
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property projectId "projectId"
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property path
    Then Select GCSMultiFile property format "csv"
    Then Enter GCSMultiFile sink property path suffix "multiFilePathSuffix"
    Then Toggle GCSMultiFile property allow flexible schemas to true
    Then Validate "GCSMultiFiles" plugin properties
    Then Close GCSMultiFile Properties
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
    Then Verify data is transferred to target GCSMultiFile bucket with path suffix "multiFilePathSuffix"

  @GCS_DATATYPE_1_TEST @GCS_SINK_TEST
  Scenario: To verify data is getting transferred from GCS to GcsMultiFile with datatypes supported
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is GCSMultiFile
    Then Connect source as "GCS" and sink as "GCSMultiFiles" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsDataTypeTest1File"
    Then Toggle GCS source property skip header to true
    Then Select GCS property format "csv"
    Then Click on the Get Schema button
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property projectId "projectId"
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property path
    Then Select GCSMultiFile property format "csv"
    Then Toggle GCSMultiFile property allow flexible schemas to true
    Then Validate "GCSMultiFiles" plugin properties
    Then Close GCSMultiFile Properties
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
