Feature: Verify Different schema

  @TC-csv
  Scenario: Verify csv file formats
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsCsvbucket" and format "gcsCSVFileFormat"
    Then Verify the schema in output

  @TC-tsv
  Scenario: Verify tsv file formats
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsTsvbucket" and format "gcsTSVFileFormat"
    Then Verify the schema in output
  @TC-avro
  Scenario: Verify avro file formats
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsavrobucket" and format "gcsavroFileFormat"
    Then Verify the schema in output
  @TC-blob
  Scenario: Verify blob different file formats
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsBlobbucket" and format "gcsblobFileFormat"
    Then Verify the schema in output

  @TC-parquet
  Scenario: Verify parquet file formats
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsParquetbucket" and format "gcsParquetFileFormat"
    Then Verify the schema in output

  @TC-delimited
  Scenario: Verify delimited different file formats
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsDelimitedbucket" and format "gcsdelimitedFileFormat"
    Then Verify the schema in output

  @TC-text
  Scenario: Verify text file formats
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is BigQuery
    Then Link Source and Sink to establish connection
    Then Enter the GCS Properties with GCS bucket "gcsTextbucket" and format "gcsTextFileFormat"
    Then Verify the schema in output
