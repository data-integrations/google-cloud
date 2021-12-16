Feature: Verification of BQ to PubSub successful data transfer

  @PubSub
  Scenario: To verify data is getting transferred from BQ to PubSub
    Given Open Datafusion Project to configure pipeline
    When  Source is BigQuery
    When  Target is PubSub
    Then  Link Source as "BigQuery" and Sink as "Pub/Sub" to establish connection
    Then  Open BigQuery Properties
    Then  Enter the BigQuery properties for table "pubSubBQTableName"
    Then  Capture and validate output schema
    Then  Close the BigQuery properties
    Then  Open the PubSub Properties
    Then  Enter the PubSub Properties for topic "pubSubTopic" and format "csv"
    Then  Enter the PubSub advanced properties
    Then  Validate the PubSub properties
    Then  Close the PubSub properties
    Then  Save the pipeline
    Then  Preview and run the pipeline
    Then  Verify the preview of pipeline is "success"
    Then  Click on PreviewData for BigQuery Connector
    Then  Verify Preview output schema matches the outputSchema captured in properties
    Then  Close the Preview
    Then  Deploy the pipeline
    Then  Run the Pipeline in Runtime
    Then  Wait till pipeline is in running state
    Then  Verify the pipeline status is "Succeeded"
    Then  Open Logs
    Then  Validate successMessage is displayed
    Then  Get Count of no of records transferred to BigQuery in "pubSubBQTableName"
