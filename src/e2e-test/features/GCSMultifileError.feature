Feature: GCS Multifile Plugin Error scenarios

  @Multifile
  Scenario Outline: Verify mandatory fields error validation in GCS multifile sink plugin
    Given Open Datafusion Project to configure pipeline
    When Target is GcsMultifile
    Then Enter the GCSMultifile properties with blank property "<property>"
    Then Verify required property error for "<property>"
    Examples:
      | property        |
      | referenceName   |
      | path            |
      | splitField      |

  @Multifile
  Scenario: To verify Error message for invalid bucket name
    Given Open Datafusion Project to configure pipeline
    When Target is GcsMultifile
    Then Enter the Gcs Multifile Properties for table "multifileInvalidPath" and format "gcsCSVFileFormat"
    Then Verify invalid path name error message is displayed for path "multifileInvalidPath"

  @Multifile
  Scenario: To Verify Valid Content Type in avro Format
    Given Open Datafusion Project to configure pipeline
    When Target is GcsMultifile
    Then Enter the Gcs Multifile Properties for table "gcsMultifilePath" and format "gcsAvroFileFormat" with ContentType "multifileJSONContentType"
    Then Verify Content Type Validation
