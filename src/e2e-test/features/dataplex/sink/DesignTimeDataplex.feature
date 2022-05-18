@Dataplex_Sink
Feature: Dataplex Sink - Design time scenarios

  @BATCH-TS-DATAPLEX-DSGN-01
  Scenario: To verify user should be able to validate mandatory fields in dataplex sink when asset type is selected as BQ dataset
    When Open Datafusion Project to configure pipeline
    And Select Sink plugin: "Dataplex" from the plugins list
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexAssetID" with value: "dataplexAssetBQ"
    And Enter input plugin property: "table" with value: "dataplexOutputTable"
    And Click plugin property: "truncateTable"
    And Click plugin property: "updateTableSchema"
    And Click plugin property: "requirePartitionFilter"
    And Validate "Dataplex" plugin properties

  @BATCH-TS-DATAPLEX-DSGN-02
  Scenario: Validate all fields in dataplex sink when asset type is selected as BigQuery dataset
    When Open Datafusion Project to configure pipeline
    And Select Sink plugin: "Dataplex" from the plugins list
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexAssetID" with value: "dataplexAssetBQ"
    And Enter input plugin property: "table" with value: "dataplexOutputTable"
    And Select radio button plugin property: "partitioningType" with value: "dataplexIntegerPartitioningRadioBtn"
    And Click plugin property: "updateTableSchema"
    And Validate "Dataplex" plugin properties

  @BATCH-TS-DATAPLEX-DSGN-03
  Scenario Outline: To verify user should be able to validate mandatory fields in dataplex sink with Storage bucket
  when different formats are selected like avro csv json
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
    And Enter input plugin property: "table" with value: "dataplexOutputTable"
    And Select dropdown plugin property: "format" with option value: "<Format>"
    And Validate "Dataplex" plugin properties
    Examples:
      | Format      |
      | avro        |
      | csv         |
      | json        |

  @BATCH-TS-DATAPLEX-DSGN-06
  Scenario Outline: Verify user should be able to validate mandatory fields with storage bucket when orc and parquet
  formats are selected one at a time
    When Open Datafusion Project to configure pipeline
    Then Select plugin: "Dataplex (Preview)" from the plugins list as: "Source"
    And Select Sink plugin: "Dataplex" from the plugins list
    And Connect plugins: "Dataplex (Preview)" and "Dataplex (Preview)2" to establish connection
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexEntityID" with value: "dataplexEntity"
    And Click on the Get Schema button
    And Validate "Dataplex" plugin properties
    And Close the Plugin Properties page
    And Navigate to the properties page of plugin: "Dataplex (Preview)2"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexAssetID" with value: "dataplexAssetStorageBucket"
    And Select radio button plugin property: "dataplexAssetType" with value: "dataplexStorageBucketRadioBtn"
    And Enter input plugin property: "table" with value: "dataplexOutputTable"
    And Select dropdown plugin property: "format" with option value: "<Format>"
    And Validate "Dataplex" plugin properties
    Examples:
      | Format      |
      | orc         |
      | parquet     |
