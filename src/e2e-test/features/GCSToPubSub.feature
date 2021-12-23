Feature: Verification of GCS to PubSub successful data transfer

  @PubSub
  Scenario: To verify data is getting transferred from GCS to PubSub
    Given Open Datafusion Project to configure pipeline
    When  Source is GCS bucket
    When  Target is PubSub
    Then  Link Source as "GCS" and Sink as "Pub/Sub" to establish connection
    Then  Open GCS properties
    Then  Enter the GCS Properties for "pubSubGCSBucket" GCS bucket with format "csv"
    Then  Capture and validate output schema
    Then  Validate the GCS properties
    Then  Close the GCS Properties
    Then  Open the PubSub Properties
    Then  Enter the PubSub Properties for topic "pubSubTopic" and format "csv"
    Then  Enter the PubSub advanced properties
    Then  Validate the PubSub properties
    Then  Close the PubSub properties
    Then  Save the pipeline
    Then  Preview and run the pipeline
    Then  Verify the preview of pipeline is "success"
    Then  Click on PreviewData for PubSub Connector
    Then  Verify Preview output schema matches the outputSchema captured in properties
    Then  Close the Preview
    Then  Deploy the pipeline
    Then  Run the Pipeline in Runtime
    Then  Wait till pipeline is in running state
    Then  Verify the pipeline status is "Succeeded"
    Then  Validate OUT record count is equal to IN record count
    Then  Open Logs
    Then  Validate successMessage is displayed

  @PubSub
  Scenario Outline: Verify data is getting transferred from GCS to PubSub for different file formats.
    Given Open Datafusion Project to configure pipeline
    When  Source is GCS bucket
    When  Target is PubSub
    Then  Link Source as "GCS" and Sink as "Pub/Sub" to establish connection
    Then  Open GCS properties
    Then  Enter the GCS Properties for "pubSubGCSBucket" GCS bucket with format "csv"
    Then  Capture and validate output schema
    Then  Validate the GCS properties
    Then  Close the GCS Properties
    Then  Open the PubSub Properties
    Then  Enter the PubSub Properties for topic "pubSubTopic" and format "<FileFormat>"
    Then  Enter the PubSub advanced properties
    Then  Validate the PubSub properties
    Then  Close the PubSub properties
    Then  Save the pipeline
    Then  Preview and run the pipeline
    Then  Verify the preview of pipeline is "success"
    Then  Click on PreviewData for PubSub Connector
    Then  Verify Preview output schema matches the outputSchema captured in properties
    Then  Close the Preview
    Then  Deploy the pipeline
    Then  Run the Pipeline in Runtime
    Then  Wait till pipeline is in running state
    Then  Verify the pipeline status is "Succeeded"
    Then  Validate OUT record count is equal to IN record count
    Then  Open Logs
    Then  Validate successMessage is displayed
    Examples:
      |FileFormat     |
      | text          |
      | avro          |
      | blob          |
      | tsv           |
      | json          |
      | parquet       |
