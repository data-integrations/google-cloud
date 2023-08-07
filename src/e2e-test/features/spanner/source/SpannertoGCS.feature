@Spanner_Source @SPANNER_TEST
Feature: Spanner Source - Verification of Spanner to GCS successful data transfer

  @GCS_SINK_TEST @Spanner_Source_Required
  Scenario: Verify data is getting transferred from Spanner to GCS successfully
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner
    When Sink is GCS
    Then Connect source as "Spanner" and sink as "GCS" to establish connection
    Then Open Spanner source properties
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter Spanner property InstanceId
    Then Enter Spanner source property DatabaseName
    Then Enter Spanner source property TableName
    Then Validate output schema with expectedSchema "spannerSourceSchema"
    Then Validate "Spanner" plugin properties
    Then Close the Spanner properties
    Then Open GCS sink properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS sink mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for GCS sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket

  @GCS_SINK_TEST
  Scenario: Verify data is getting transferred from Spanner to GCS with Import Query.
    Given Open Datafusion Project to configure pipeline
    When Source is Spanner
    When Sink is GCS
    Then Connect source as "Spanner" and sink as "GCS" to establish connection
    Then Open Spanner source properties
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter Spanner property InstanceId
    Then Enter Spanner source property DatabaseName
    Then Enter Spanner source property TableName
    Then Enter the Spanner source property Import Query "spannerQuery"
    Then Validate output schema with expectedSchema "spannerSourceSchema"
    Then Validate "Spanner" plugin properties
    Then Close the Spanner properties
    Then Open GCS sink properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS sink mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for GCS sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
