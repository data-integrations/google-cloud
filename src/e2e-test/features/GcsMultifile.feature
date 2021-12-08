Feature: Multifile

  @TC-BigQueryGCSMultifile
  Scenario: To verify data is getting transferred from BigQuery to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multifileBQTable" amd dataset "dataset" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "gcsCSVFileFormat"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

  @TC-GCS-GCSMultiflow
  Scenario: To verify data is getting transferred from GCS to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is GcsMultifile
    Then Link GCS and GCSMultiFile to establish connection
    Then Enter the GCS Properties with "multifileGCSBucket" GCS bucket
    Then Close the GCS Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "gcsCSVFileFormat"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

  @TC-MultipleSource-GCSMultifile
  Scenario: To verify data is getting transferred from MultipleSource to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is GcsMultifile
    Then Link GCS and GCSMultiFile to establish connection
    Then Enter the GCS Properties with "multifileMultipleSource" GCS bucket
    Then Close the GCS Properties
    Then Click on Source
    Then Source is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multitable" amd dataset "dataset" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "gcsCSVFileFormat"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

  @TC-GCSMultifile_differentFormats
  Scenario Outline: To verify data is getting transferred from BigQuery to GcsMultifile with different file Formats
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multitable" amd dataset "dataset" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "<format>"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed
    Examples:
    | format            |
    | gcsCSVFileFormat  |
    | gcsAvroFileFormat |
    | gcsParquetFileFormat  |
    | gcsTextFileFormat     |
    | multifileOrcFormat    |
    | gcsTSVFileFormat      |


  @TC-FileFormat-delimited
  Scenario: To verify data is getting transferred from BigQuery to GcsMultifile with Delimited Format
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multitable" amd dataset "dataset" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and delimited format "gcsDelimitedFileFormat" and delimiter "multifileDelimiter"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

  @TC-CodecFormat
  Scenario: To verify data is getting transferred from BigQuery to GcsMultifile using Codec format
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multitable" amd dataset "dataset" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath", format "gcsAvroFileFormat" with Codec "codecType"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

