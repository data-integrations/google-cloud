Feature: GCS MultiFile Plugin Error scenarios

  @MultiFile
  Scenario Outline: Verify mandatory fields error validation in GCS MultiFile sink plugin
    Given Open Datafusion Project to configure pipeline
    When Target is GcsMultiFile
    Then Enter the GCSMultiFile properties with blank property "<property>"
    Then Verify required property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | path            |
      | splitField      |

  @MultiFile
  Scenario: To verify Error message for invalid bucket name
    Given Open Datafusion Project to configure pipeline
    When Target is GcsMultiFile
    Then Enter the Gcs MultiFile Properties for table "multiFileInvalidPath" and format "gcsCSVFileFormat"
    Then Verify invalid path name error message is displayed for path "multiFileInvalidPath"

  @MultiFile
  Scenario: To Verify Valid Content Type in avro Format
    Given Open Datafusion Project to configure pipeline
    When Target is GcsMultiFile
    Then Enter the Gcs MultiFile Properties for table "multiFileGcsPath" and format "gcsAvroFileFormat" with ContentType "multiFileJSONContentType"
    Then Verify Content Type Validation
