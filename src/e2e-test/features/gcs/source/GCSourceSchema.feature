@GCS_Source
Feature: Validate GCS plugin output schema for different formats

  Scenario Outline:GCS Source output schema validation
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
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
    @GCS_BLOB_TEST
    Examples:
      | GcsPath      | FileFormat  | ExpectedSchema     |
      | gcsBlobFile  | blob        | gcsBlobFileSchema  |

  @GCS_Source
  Scenario Outline:GCS Source output schema validation for delimited files
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "<GcsPath>"
    Then Select GCS property format "<FileFormat>"
    Then Enter GCS property delimiter "<Delimiter>"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "<ExpectedSchema>"
    @GCS_DELIMITED_TEST
    Examples:
      | GcsPath           | FileFormat  | Delimiter     | ExpectedSchema         |
      | gcsDelimitedFile  | delimited   | gcsDelimiter  | gcsDelimitedFileSchema |
    @GCS_TEXT_TEST
    Examples:
      | GcsPath           | FileFormat  | Delimiter     | ExpectedSchema         |
      | gcsTextFile       | text        | gcsDelimiter  | gcsTextFileSchema      |
