Feature: Spanner to GCS data transfer for different file formats

  @Spanner
  Scenario Outline:: Verify data is getting transferred from Spanner to GCS for different file formats.
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner Connector
    When Target is GCS
    Then Connect Source as "Spanner" and sink as "GCS" to establish connection
    Then Open Spanner connector properties
    Then Enter the Spanner connector Properties
    Then Capture and validate output schema
    Then Validate Spanner connector properties
    Then Close the Spanner Properties
    Then Enter the GCS Properties and "<FileFormat>" file format
    Then Validate GCS properties
    Then Close the GCS Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for GCS connector
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed
    Examples:
      | FileFormat  |
      | csv         |
      | avro        |
      | parquet     |
      | tsv         |

  @Spanner
  Scenario Outline:: Verify data is getting transferred from Spanner to GCS for different file formats with Import Query.
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner Connector
    When Target is GCS
    Then Connect Source as "Spanner" and sink as "GCS" to establish connection
    Then Open Spanner connector properties
    Then Enter the Spanner connector Properties with Import Query "spannerQuery"
    Then Capture and validate output schema
    Then Validate Spanner connector properties
    Then Close the Spanner Properties
    Then Enter the GCS Properties and "<FileFormat>" file format
    Then Validate GCS properties
    Then Close the GCS Properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for GCS connector
    Then Verify Preview output schema matches the outputSchema captured in properties
    Then Close the Preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open Logs
    Then Validate successMessage is displayed
    Examples:
      | FileFormat  |
      | csv         |
      | avro        |
      | parquet     |
      | tsv         |
