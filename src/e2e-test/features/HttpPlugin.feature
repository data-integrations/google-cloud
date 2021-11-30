Feature: HTTP Plugin Testcases

  @HTTP-TC
  Scenario: TC-HTTP-01:Verify HTTP plugin source mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    Then Open HTTP Properties
    Then Verify mandatory fields
    Then Close the HTTP Properties

  @HTTP-TC
  Scenario: TC-HTTP-02:Verify HTTP plugin sink mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Target is HTTP
    Then Open HTTP Properties
    Then Verify mandatory fields
    Then Close the HTTP Properties

  @HTTP-TC
  Scenario: TC-HTTP-03:User is able to Login and confirm data is getting transferred from HTTP to BigQuery
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    When Target is BigQuery
    Then Link Source HTTP and Sink Bigquery to establish connection
    Then Open HTTP Properties
    Then Enter the HTTP Properties with "httpSrcValidOutputSchema" and "httpSrcValidJsonFieldsMapping"
    Then Close the HTTP Properties
    Then Enter the BigQuery Properties for table "HTTPTestA"
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Get Count of no of records transferred to BigQuery in "HTTPTestA"

  @HTTP-TC
  Scenario: TC-HTTP-04:Negative- Verify Pipeline is getting failed for invalid HTTP output schema
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    When Target is BigQuery
    Then Link Source HTTP and Sink Bigquery to establish connection
    Then Open HTTP Properties
    Then Enter the HTTP Properties with "httpSrcInvalidOutputSchema" and "httpSrcValidJsonFieldsMapping"
    Then Close the HTTP Properties
    Then Enter the BigQuery Properties for table "HTTPTestA"
    Then Close the BigQuery Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Failed"

  @HTTP-TC
  Scenario: TC-HTTP-02:Verify HTTP plugin validation error for invalid jsonpath and outputSchema mapping
    Given Open Datafusion Project to configure pipeline
    When Source is HTTP
    Then Open HTTP Properties
    Then Enter the HTTP Properties with "httpSrcBlankOutputSchema" and "httpSrcInvalidJsonFieldsMapping"
    Then Verify validation error
    Then Close the HTTP Properties

  @HTTP-TC
  Scenario: TC-HTTP-04:User is able to Login and confirm data is getting transferred from GCS to HTTP
    Given Open Datafusion Project to configure pipeline
    When Source is GCS bucket
    When Target is HTTP
    Then Link Source GCS and Sink HTTP to establish connection
    Then Enter the GCS Properties with "httpGSCBucket" GCS bucket
    Then Close the GCS Properties
    Then Open HTTP Properties
    Then Enter the HTTP Sink Properties
    Then Close the HTTP Properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"


