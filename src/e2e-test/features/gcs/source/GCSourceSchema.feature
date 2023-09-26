@GCS_Source
Feature: GCS source - Validate GCS plugin output schema for different formats

  Scenario Outline:GCS Source output schema validation for csv and tsv format
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "<GcsPath>"
    Then Select GCS property format "<FileFormat>"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "<ExpectedSchema>"
    @GCS_CSV_TEST
    Examples:
      | GcsPath      | FileFormat  | ExpectedSchema     |
      | gcsCsvFile   | csv         | gcsCsvFileSchema   |
    @GCS_TSV_TEST
    Examples:
      | GcsPath      | FileFormat  | ExpectedSchema     |
      | gcsTsvFile   | tsv         | gcsTsvFileSchema   |

  @GCS_Source_Required
  Scenario Outline:GCS Source output schema validation for blob, parquet, avro and text format
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "<GcsPath>"
    Then Select GCS property format "<FileFormat>"
    Then Validate output schema with expectedSchema "<ExpectedSchema>"
    @GCS_BLOB_TEST
    Examples:
      | GcsPath      | FileFormat  | ExpectedSchema     |
      | gcsBlobFile  | blob        | gcsBlobFileSchema  |
    @GCS_TEXT_TEST
    Examples:
      | GcsPath      | FileFormat  | ExpectedSchema     |
      | gcsTextFile  | text        | gcsTextFileSchema  |
    @GCS_PARQUET_TEST @CDAP-18494
    Examples:
      | GcsPath        | FileFormat | ExpectedSchema       |
      | gcsParquetFile | parquet    | gcsParquetFileSchema |
    @GCS_AVRO_TEST @CDAP-18494
    Examples:
      | GcsPath        | FileFormat | ExpectedSchema       |
      | gcsAvroFile    | avro       | gcsAvroFileSchema    |

  @GCS_Source
  Scenario Outline:GCS Source output schema validation for delimited files
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "<GcsPath>"
    Then Select GCS property format "<FileFormat>"
    Then Enter GCS property delimiter "<Delimiter>"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "<ExpectedSchema>"
    @GCS_DELIMITED_TEST
    Examples:
      | GcsPath          | FileFormat | Delimiter             | ExpectedSchema                |
      | gcsDelimitedFile | delimited  | gcsDelimiter          | gcsDelimitedFileSchema        |
      | gcsDelimitedFile | delimited  | gcsIncorrectDelimiter | gcsOutputSchemaWithUnderscore |

  @GCS_Source @GCS_DELIMITED_TEST
  Scenario:GCS Source output schema validation for delimited files without delimiter field
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Override Service account details if set in environment variables
    Then Enter GCS source property path "gcsDelimitedFile"
    Then Select GCS property format "delimited"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsOutputSchemaWithUnderscore"

  @GCS_DATATYPE_1_TEST @PLUGIN-825
  Scenario Outline:GCS Source automatic data type detection for Timestamp datatype
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "<GcsPath>"
    Then Select GCS property format "<FileFormat>"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "<ExpectedSchema>"
    Examples:
      | GcsPath              | FileFormat | ExpectedSchema                    |
      | gcsDataTypeTest1File | csv        | gcsOverrideString_TimestampSchema |
