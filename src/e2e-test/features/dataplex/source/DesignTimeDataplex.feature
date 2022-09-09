Feature: Dataplex Source - Design time scenarios

  @BATCH-TS-DATAPLEX-DSGN-01
  Scenario: To validate data when pipeline is created between DataPlex to DataPlex
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
    And Validate output schema with expectedSchema "dataplexSourceSchema"
    And Validate "Dataplex" plugin properties
    And Close the Plugin Properties page
    And Navigate to the properties page of plugin: "Dataplex (Preview)2"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexAssetID" with value: "dataplexAssetBQ"
    And Enter input plugin property: "table" with value: "dataplexOutputTable"
    And Validate "Dataplex" plugin properties
    And Close the Plugin Properties page

  @BATCH-TS-DATAPLEX-DSGN-02
  Scenario: To verify user should be able to validate output schema successfully in dataplex source
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
    And Validate output schema with expectedSchema "dataplexSourceSchema"
    And Validate "Dataplex" plugin properties

  @BATCH-TS-DATAPLEX-DSGN-03
  Scenario: To verify user should be able to validate output schema with partition dates
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
    And Enter input plugin property: "partitionStartDate" with value: "dataplexPartitionDate"
    And Enter input plugin property: "partitionEndDate" with value: "dataplexPartitionToDate"
    And Validate output schema with expectedSchema "dataplexSourceSchema"
    And Validate "Dataplex" plugin properties

  @BATCH-TS-DATAPLEX-DSGN-04
  Scenario: To verify user should be able to filter the output schema in dataplex source
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
    And Enter input plugin property: "filter" with value: "dataplexFilter"
    And Validate output schema with expectedSchema "dataplexSourceSchema"
    And Validate "Dataplex" plugin properties
