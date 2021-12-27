Feature: MultiFile

  @GCSMultiFile
  Scenario: To verify data is getting transferred from BigQuery to GcsMultiFile
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GcsMultiFile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multiFileBQTable" amd dataset "dataset" for source
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Enter the Gcs MultiFile Properties for table "multiFileGcsPath" and format "gcsCSVFileFormat"
    Then Validate GCS MultiFile properties
    Then Close Gcs MultiFile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed

  @GCSMultiFile
  Scenario: To verify data is getting transferred from GCS to GcsMultiFile
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is GcsMultiFile
    Then Link GCS and GCSMultiFile to establish connection
    Then Enter the GCS Properties with "multiFileGCSBucket" GCS bucket
    Then Validate GCS properties
    Then Close the GCS Properties
    Then Enter the Gcs MultiFile Properties for table "multiFileGcsPath" and format "gcsCSVFileFormat"
    Then Validate GCS MultiFile properties
    Then Close Gcs MultiFile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed

  @GCSMultiFile
  Scenario: To verify data is getting transferred from MultipleSource to GcsMultiFile
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is GcsMultiFile
    Then Link GCS and GCSMultiFile to establish connection
    Then Enter the GCS Properties with "multiFileMultipleSource" GCS bucket
    Then Validate GCS properties
    Then Close the GCS Properties
    Then Click on Source
    Then Source is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multiFileMultiTable" amd dataset "dataset" for source
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Enter the Gcs MultiFile Properties for table "multiFileGcsPath" and format "gcsCSVFileFormat"
    Then Validate GCS MultiFile properties
    Then Close Gcs MultiFile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed

  @GCSMultiFile
  Scenario Outline: To verify data is getting transferred from BigQuery to GcsMultiFile with different file Formats
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GcsMultiFile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multiFileMultiTable" amd dataset "dataset" for source
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Enter the Gcs MultiFile Properties for table "multiFileGcsPath" and format "<format>"
    Then Validate GCS MultiFile properties
    Then Close Gcs MultiFile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed
    Examples:
    | format                |
    | gcsCSVFileFormat      |
    | gcsAvroFileFormat     |
    | gcsParquetFileFormat  |
    | gcsJsonFileFormat     |
    | multiFileOrcFormat    |
    | gcsTSVFileFormat      |

  @GCSMultiFile
  Scenario: To verify data is getting transferred from BigQuery to GcsMultifile with Delimited Format
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GcsMultiFile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multiFileMultiTable" amd dataset "dataset" for source
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Enter the Gcs MultiFile Properties for table "multiFileGcsPath" and delimited format "gcsDelimitedFileFormat" and delimiter "multiFileDelimiter"
    Then Validate GCS MultiFile properties
    Then Close Gcs MultiFile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed

  @GCSMultiFile
  Scenario: To verify data is getting transferred from BigQuery to GcsMultiFile using Codec format
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GcsMultiFile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multiFileMultiTable" amd dataset "dataset" for source
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Enter the Gcs MultiFile Properties for table "multiFileGcsPath", format "gcsAvroFileFormat" with Codec "multiFileCodecType"
    Then Validate GCS MultiFile properties
    Then Close Gcs MultiFile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed

  @GCSMultiFile
  Scenario: To verify data is getting transferred successfully from BigQuery to GcsMultiFile and then GCS to BigQuery
    Given Open Datafusion Project to configure pipeline
    Given Cloud Storage bucket should not exist in "projectId" with the name "multiFileBucketCreate"
    When Source is BigQuery
    When Target is GcsMultiFile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multiFileBQTable" amd dataset "dataset" for source
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Enter the Gcs MultiFile Properties for table "multiFileGcsPath" and format "gcsCSVFileFormat"
    Then Validate GCS MultiFile properties
    Then Close Gcs MultiFile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Verify the folder created in "projectId" with bucket name "multiFileBucketCreate"
    Then Open Logs
    Then Validate successMessage is displayed
    Then Get the count of the records transferred
    Then Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is selected as BigQuery
    Then Link Source GCS and Sink bigquery to establish connection
    Then Enter the GCS Properties with "multiFileGcsPath" GCS bucket and skip header
    Then Validate GCS properties
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for Sink "multiFileGcsBqTableName"
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed
    Then Get Count of no of records transferred to BigQuery from GCS "multiFileGcsBqTableName"
