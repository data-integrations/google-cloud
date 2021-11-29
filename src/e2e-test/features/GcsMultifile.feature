Feature: Multifile

  @TC-BigQueryGCSMultiflow
  Scenario: User is able to Login and confirm data is getting transferred from BigQuery to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery bucket
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "table" amd dataset "dataset" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "avro"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed



  @TC-GCS-GCSMultiflow
  Scenario: User is able to Login and confirm data is getting transferred from BigQuery to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is GcsMultifile
    Then Link GCS and GCSMultiFile to establish connection
    Then Enter the GCS Properties with "@TC-Demo-1_GCS" GCS bucket
    Then Close the GCS Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "tsv"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

  @TC-MultipleSource-GCSMultiflow
  Scenario: User is able to Login and confirm data is getting transferred from BigQuery to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is GcsMultifile
    Then Link GCS and GCSMultiFile to establish connection
    Then Enter the GCS Properties with "@TC-Demo-1_GCS" GCS bucket
    Then Close the GCS Properties
    Then Click on Source
    Then Source is BigQuery bucket
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "multitable" amd dataset "dataset" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "csv"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

  @TC-FileFormat-tsv
  Scenario: User is able to Login and confirm data is getting transferred from BigQuery to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery bucket
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "Employeedata" amd dataset "test_automation" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "tsv"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

  @TC-FileFormat-avro
  Scenario: User is able to Login and confirm data is getting transferred from BigQuery to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery bucket
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "Employeedata" amd dataset "test_automation" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "avro"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

  @TC-FileFormat-Parquet
  Scenario: User is able to Login and confirm data is getting transferred from BigQuery to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery bucket
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "Employeedata" amd dataset "test_automation" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "parquet"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

  @TC-FileFormat-text
  Scenario: User is able to Login and confirm data is getting transferred from BigQuery to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery bucket
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "Employeedata" amd dataset "test_automation" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "text"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

  @TC-FileFormat-Orc
  Scenario: User is able to Login and confirm data is getting transferred from BigQuery to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery bucket
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "Employeedata" amd dataset "test_automation" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "orc"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

  @TC-FileFormat-tsv
  Scenario: User is able to Login and confirm data is getting transferred from BigQuery to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery bucket
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "Employeedata" amd dataset "test_automation" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "tsv"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

  @TC-FileFormat-delimited
  Scenario: User is able to Login and confirm data is getting transferred from BigQuery to GcsMultifile
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery bucket
    When Target is GcsMultifile
    Then Link Source and Sink to establish connection
    Then Enter the BigQuery Properties for table "Employeedata" amd dataset "test_automation" for source
    Then Close the BigQuery Properties
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "delimited"
    Then Close Gcs Multifile Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open Logs
    Then Verify the pipeline status is "Succeeded"
    Then validate successMessage is displayed

