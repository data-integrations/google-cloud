@PubSub_Sink @PUBSUB_SINK_TEST
Feature: PubSub-Sink - Verification of GCS to PubSub successful data transfer

  @CMEK @GCS_CSV_TEST
  Scenario: To verify data is getting transferred from GCS to PubSub successfully
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is PubSub
    Then Open GCS source properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS source mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Select PubSub property format "csv"
    Then Enter PubSub sink property encryption key name "cmekPubSub" if cmek is enabled
    Then Enter PubSub sink property MaximumBatchCount "pubSubMaximumBatchCount"
    Then Enter PubSub sink property MaximumBatchSize "pubSubMaximumBatchSize"
    Then Enter PubSub sink property PublishDelayThreshold "pubSubPublishDelayThreshold"
    Then Enter PubSub sink property RetryTimeOut "pubSubRetryTimeOut"
    Then Enter PubSub sink property ErrorThreshold "pubSubErrorThreshold"
    Then Validate "PubSub" plugin properties
    Then Close the PubSub properties
    Then Connect source as "GCS" and sink as "GooglePublisher" to establish connection
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
    Then Validate the cmek key "cmekPubSub" of target PubSub topic if cmek is enabled

  @GCS_CSV_TEST
  Scenario Outline: Verify data is getting transferred from GCS to PubSub for different file formats.
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is PubSub
    Then Open GCS source properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS source mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Select PubSub property format "<FileFormat>"
    Then Enter PubSub sink property MaximumBatchCount "pubSubMaximumBatchCount"
    Then Enter PubSub sink property MaximumBatchSize "pubSubMaximumBatchSize"
    Then Enter PubSub sink property PublishDelayThreshold "pubSubPublishDelayThreshold"
    Then Enter PubSub sink property RetryTimeOut "pubSubRetryTimeOut"
    Then Enter PubSub sink property ErrorThreshold "pubSubErrorThreshold"
    Then Validate "PubSub" plugin properties
    Then Close the PubSub properties
    Then Connect source as "GCS" and sink as "GooglePublisher" to establish connection
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
    Examples:
      | FileFormat |
      | text       |
      | avro       |
      | blob       |
      | tsv        |
      | json       |
      | parquet    |

  @GCS_DATATYPE_1_TEST
  Scenario: To verify data is getting transferred from GCS to PubSub with supported DataTypes
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is PubSub
    Then Connect source as "GCS" and sink as "GooglePublisher" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsDataTypeTest1File"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Click on the Get Schema button
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Select PubSub property format "csv"
    Then Validate "PubSub" plugin properties
    Then Close the PubSub properties
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

  @GCS_DATATYPE_2_TEST
  Scenario: To verify data is getting transferred from GCS to PubSub with supported DataTypes with special cases of float
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is PubSub
    Then Connect source as "GCS" and sink as "GooglePublisher" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsDataTypeTest2File"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Click on the Get Schema button
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Select PubSub property format "csv"
    Then Validate "PubSub" plugin properties
    Then Close the PubSub properties
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

  @PubSub_Sink_Required
  Scenario Outline: To verify data is getting transferred from GCS to PubSub with different file format combination in source and sink
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is PubSub
    Then Connect source as "GCS" and sink as "GooglePublisher" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "<GcsPath>"
    Then Select GCS property format "<SourceFormat>"
    Then Toggle GCS source property skip header to true
    Then Click on the Get Schema button
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Select PubSub property format "<SinkFormat>"
    Then Validate "PubSub" plugin properties
    Then Close the PubSub properties
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
    @GCS_CSV_TEST
    Examples:
      | GcsPath    | SourceFormat | SinkFormat |
      | gcsCsvFile | csv          | blob       |
    @GCS_TSV_TEST
    Examples:
      | GcsPath    | SourceFormat | SinkFormat |
      | gcsTsvFile | tsv          | parquet    |
    @GCS_CSV_TEST
    Examples:
      | GcsPath    | SourceFormat | SinkFormat |
      | gcsCsvFile | csv          | json       |
    @GCS_TSV_TEST
    Examples:
      | GcsPath    | SourceFormat | SinkFormat |
      | gcsTsvFile | tsv          | avro       |
    @GCS_CSV_TEST
    Examples:
      | GcsPath    | SourceFormat | SinkFormat |
      | gcsCsvFile | csv          | tsv        |

  @PubSub_Sink_Required
  Scenario Outline: To verify data is getting transferred from GCS to PubSub with different text format in source and delimited format in Sink
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is PubSub
    Then Connect source as "GCS" and sink as "GooglePublisher" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "<GcsPath>"
    Then Select GCS property format "<SourceFormat>"
    Then Click on the Get Schema button
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Select PubSub property format "<SinkFormat>"
    Then Enter PubSub property delimiter "<Delimiter>"
    Then Validate "PubSub" plugin properties
    Then Close the PubSub properties
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
    @GCS_TEXT_TEST
    Examples:
      | GcsPath     | SourceFormat | SinkFormat | Delimiter       |
      | gcsTextFile | text         | delimited  | pubsubDelimiter |
