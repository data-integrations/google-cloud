@Dataplex_Sink
Feature: Dataplex Sink - Verify error Scenarios

  @BATCH-TS-DATAPLEX-Error-01
  Scenario: Verify dataplex sink properties validation errors for mandatory fields
    When Open Datafusion Project to configure pipeline
    And Select Sink plugin: "Dataplex" from the plugins list
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Click on the Validate button
    And Verify mandatory property error for below listed properties:
      | referenceName |
      | location      |
      | lake          |
      | zone          |
      | asset         |

  @BATCH-TS-DATAPLEX-Error-02
  Scenario: Verify dataplex sink properties for asset details errors when asset type is bigquery dataset
    When Open Datafusion Project to configure pipeline
    And Select Sink plugin: "Dataplex" from the plugins list
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexAssetID" with value: "dataplexAssetBQ"
    And Click plugin property: "truncateTable"
    And Click plugin property: "updateTableSchema"
    And Click on the Validate button
    And Verify mandatory property error for below listed properties:
      | table |

  @BATCH-TS-DATAPLEX-Error-03
  Scenario: Verify dataplex sink properties for asset details errors when asset type is Storage bucket
    When Open Datafusion Project to configure pipeline
    And Select Sink plugin: "Dataplex" from the plugins list
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexAssetID" with value: "dataplexAssetStorageBucket"
    And Select radio button plugin property: "dataplexAssetType" with value: "dataplexStorageBucketRadioBtn"
    And Click on the Validate button
    And Verify mandatory property error for below listed properties:
      | table |

  @BATCH-TS-DATAPLEX-Error-04
  Scenario: Validate invalid asset type error message when asset type is storage bucket
    When Open Datafusion Project to configure pipeline
    And Select Sink plugin: "Dataplex" from the plugins list
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexAssetID" with value: "dataplexInvalidStorageAssetValue"
    And Select radio button plugin property: "dataplexAssetType" with value: "dataplexStorageBucketRadioBtn"
    And Enter input plugin property: "table" with value: "dataplexOutputTable"
    And Click on the Validate button
    And Verify that the Plugin Property: "asset" is displaying an in-line error message: "dataplexInvalidSBAssetError"

  @BATCH-TS-DATAPLEX-Error-05
  Scenario: Validate invalid asset type error message when asset type is bigquery dataset
    When Open Datafusion Project to configure pipeline
    And Select Sink plugin: "Dataplex" from the plugins list
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexAssetID" with value: "dataplexInvalidBQasset"
    And Enter input plugin property: "table" with value: "dataplexOutputTable"
    And Click on the Validate button
    And Verify that the Plugin Property: "asset" is displaying an in-line error message: "dataplexInvalidBQassetError"
