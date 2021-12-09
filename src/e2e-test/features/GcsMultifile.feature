Feature: Multifile

  @GCSMultifile
  Scenario: To verify data is getting transferred from BigQuery to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multifileBQTable" amd dataset "dataset" for source
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "gcsCSVFileFormat"
    Then Validate GCS Multifile properties
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then validate successMessage is displayed

  @GCSMultifile
  Scenario: To verify data is getting transferred from GCS to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is GcsMultifile
    Then Link GCS and GCSMultiFile to establish connection
    Then Enter the GCS Properties with "multifileGCSBucket" GCS bucket
    Then Validate GCS properties
    Then Close the GCS Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "gcsCSVFileFormat"
    Then Validate GCS Multifile properties
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then validate successMessage is displayed

  @GCSMultifile
  Scenario: To verify data is getting transferred from MultipleSource to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is GcsMultifile
    Then Link GCS and GCSMultiFile to establish connection
    Then Enter the GCS Properties with "multifileMultipleSource" GCS bucket
    Then Validate GCS properties
    Then Close the GCS Properties
    Then Click on Source
    Then Source is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multifileMultitable" amd dataset "dataset" for source
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "gcsCSVFileFormat"
    Then Validate GCS Multifile properties
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then validate successMessage is displayed

  @GCSMultifile
  Scenario Outline: To verify data is getting transferred from BigQuery to GcsMultifile with different file Formats
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multifileMultitable" amd dataset "dataset" for source
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "<format>"
    Then Validate GCS Multifile properties
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then validate successMessage is displayed
    Examples:
    | format            |
    | gcsCSVFileFormat  |
    | gcsAvroFileFormat |
    | gcsParquetFileFormat  |
    | gcsTextFileFormat     |
    | multifileOrcFormat    |
    | gcsTSVFileFormat      |

  @GCSMultifile
  Scenario: To verify data is getting transferred from BigQuery to GcsMultifile with Delimited Format
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multifileMultitable" amd dataset "dataset" for source
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and delimited format "gcsDelimitedFileFormat" and delimiter "multifileDelimiter"
    Then Validate GCS Multifile properties
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then validate successMessage is displayed

  @GCSMultifile
  Scenario: To verify data is getting transferred from BigQuery to GcsMultifile using Codec format
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multifileMultitable" amd dataset "dataset" for source
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath", format "gcsAvroFileFormat" with Codec "multifilecodecType"
    Then Validate GCS Multifile properties
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then validate successMessage is displayed

  @GCSMultifile
  Scenario: To verify data is getting transferred successfully from BigQuery to GcsMultifile and then GCS to BigQuery
    Given Open Datafusion Project to configure pipeline
    Given Cloud Storage bucket should not exist in "projectId" with the name "multifilebucketcreate"
    When Source is BigQuery
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multifileBQTable" amd dataset "dataset" for source
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "gcsCSVFileFormat"
    Then Validate GCS Multifile properties
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Verify the folder created in "projectId" with bucket name "multifilebucketcreate"
    Then Open Logs
    Then validate successMessage is displayed
    Then Get the count of the records transferred
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is selected as BigQuery
    Then Link Source GCS and Sink bigquery to establish connection
    Then Enter the GCS Properties with "gcsMultifilePath" GCS bucket and skip header
    Then Validate GCS properties
    Then Close the GCS Properties
    Then Enter the BigQuery Properties for Sink "gcsBqTableName"
    Then Validate BigQuery properties
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then validate successMessage is displayed
    Then Get Count of no of records transferred to BigQuery from GCS "gcsBqTableName"
