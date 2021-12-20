Feature: Verify PubSub Plugin error Scenarios

  @PubSub
  Scenario Outline: Verify PubSub sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Target is PubSub
    Then Open the PubSub Properties
    Then Enter the PubSub property with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property      |
      | referenceName |
      | topic         |

  @PubSub
  Scenario Outline: Verify errors for incorrect values in pubsub advanced properties
    Given Open Datafusion Project to configure pipeline
    When Target is PubSub
    Then Open the PubSub Properties
    Then Enter the PubSub Properties for topic "pubSubTopic" and format "csv"
    Then Enter the PubSub advanced properties with incorrect property "<Property>"
    Then Validate the error message for property "<Property>"
    Examples:
      | Property                    |
      | messageCountBatchSize       |
      | requestThresholdKB          |
      | publishDelayThresholdMillis |
      | retryTimeoutSeconds         |
      | errorThreshold              |

  @PubSub
  Scenario Outline: Verify errors for negative values  pubsub advanced properties
    Given Open Datafusion Project to configure pipeline
    When Target is PubSub
    Then Open the PubSub Properties
    Then Enter the PubSub Properties for topic "pubSubTopic" and format "csv"
    Then Enter the PubSub advanced properties with invalid number for property "<Property>"
    Then Validate the number format error message for property "<Property>"
    Examples:
      | Property                    |
      | messageCountBatchSize       |
      | requestThresholdKB          |
      | publishDelayThresholdMillis |
      | retryTimeoutSeconds         |
      | errorThreshold              |
