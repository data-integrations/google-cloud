@MultiFile
Feature: GCSMultiFile Sink - GCSMultiFile Sink Plugin Error scenarios

  Scenario Outline: Verify mandatory fields error validation in GCSMultiFile sink plugin
    Given Open Datafusion Project to configure pipeline
    When Sink is GCSMultiFile
    Then Open GCSMultiFile sink properties
    Then Enter the GCSMultiFile properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property      |
      | referenceName |
      | path          |
      | splitField    |

  Scenario: To Verify GCSMultiFile invalid path name
    Given Open Datafusion Project to configure pipeline
    When Sink is GCSMultiFile
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property projectId "projectId"
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property path "multiFileInvalidPath"
    Then Select GCSMultiFile property format "csv"
    Then Verify GCSMultiFile invalid path name "multiFileInvalidPath"

  Scenario: To Verify Valid Content Type in avro Format
    Given Open Datafusion Project to configure pipeline
    When Sink is GCSMultiFile
    Then Open GCSMultiFile sink properties
    Then Enter GCSMultiFile property projectId "projectId"
    Then Enter GCSMultiFile property reference name
    Then Enter GCSMultiFile property path "multiFileGcsPath"
    Then Select GCSMultiFile property format "avro"
    Then Select GCSMultiFile property ContentType "application/json"
    Then Verify GCSMultiFile Content Type validation error
