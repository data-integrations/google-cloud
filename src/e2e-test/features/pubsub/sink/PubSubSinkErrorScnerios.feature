@PubSub_Sink
Feature: PubSub-Sink - Verify PubSub sink plugin error Scenarios

  Scenario Outline: Verify PubSub sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Sink is PubSub
    Then Open the PubSub sink properties
    Then Enter the PubSub sink properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property      |
      | referenceName |
      | topic         |

  Scenario Outline: Verify errors for incorrect values in pubsub advanced properties
    Given Open Datafusion Project to configure pipeline
    When Sink is PubSub
    Then Open the PubSub sink properties
    Then Enter PubSub property reference name
    Then Enter PubSub property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter PubSub topic name "dummytopic"
    Then Select PubSub property format "csv"
    Then Enter the PubSub sink advanced properties with incorrect property "<Property>"
    Then Validate the error message for invalid PubSub sink advanced property "<Property>"
    Examples:
      | Property                    |
      | messageCountBatchSize       |
      | requestThresholdKB          |
      | publishDelayThresholdMillis |
      | retryTimeoutSeconds         |
      | errorThreshold              |

  Scenario Outline: Verify errors for negative values pubsub advanced properties
    Given Open Datafusion Project to configure pipeline
    When Sink is PubSub
    Then Open the PubSub sink properties
    Then Enter PubSub property reference name
    Then Enter PubSub property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter PubSub topic name "dummytopic"
    Then Select PubSub property format "csv"
    Then Enter the PubSub sink advanced properties with invalid number for property "<Property>"
    Then Validate the number format error message for PubSub sink property "<Property>"
    Examples:
      | Property                    |
      | messageCountBatchSize       |
      | requestThresholdKB          |
      | publishDelayThresholdMillis |
      | retryTimeoutSeconds         |
      | errorThreshold              |
