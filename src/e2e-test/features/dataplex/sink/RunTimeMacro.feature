@Dataplex_Sink
Feature: Dataplex Sink - Verification of pipeline preview and deploy with macros

  @BATCH-TS-DATAPLEX-MACRO-01
  Scenario: When the asset type is selected as bigquery dataset, verify that the pipeline preview data and pipeline
  deployed from dataplex to dataplex with macro parameters is successful.
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
    And Click on the Macro button of Property: "projectId" and set the value to: "project"
    And Click on the Macro button of Property: "dataplexLocationID" and set the value to: "location"
    And Click on the Macro button of Property: "dataplexLakeID" and set the value to: "lake"
    And Click on the Macro button of Property: "dataplexZoneID" and set the value to: "zone"
    And Click on the Macro button of Property: "dataplexAssetID" and set the value to: "asset"
    And Click on the Macro button of Property: "table" and set the value to: "table"
    And Click on the Macro button of Property: "truncateTableMacroInput" and set the value to: "truncateTable"
    And Click on the Macro button of Property: "updateTableSchemaMacroInput" and set the value to: "updateTableSchema"
    And Click on the Macro button of Property: "requirePartitionFilterMacroInput" and set the value to: "partitionFilter"
    And Validate "Dataplex" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "projectId" for key "project"
    And Enter runtime argument value "dataplexLocation" for key "location"
    And Enter runtime argument value "dataplexLake" for key "lake"
    And Enter runtime argument value "dataplexZone" for key "zone"
    And Enter runtime argument value "dataplexAssetBQ" for key "asset"
    And Enter runtime argument value "dataplexOutputTable" for key "table"
    And Enter runtime argument value "dataplexTruncateTableTrue" for key "truncateTable"
    And Enter runtime argument value "dataplexUpdateSchemaTrue" for key "updateTableSchema"
    And Enter runtime argument value "dataplexPartitionFilterTrue" for key "partitionFilter"
    And Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    And Deploy the pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "projectId" for key "project"
    And Enter runtime argument value "dataplexLocation" for key "location"
    And Enter runtime argument value "dataplexLake" for key "lake"
    And Enter runtime argument value "dataplexZone" for key "zone"
    And Enter runtime argument value "dataplexAssetBQ" for key "asset"
    And Enter runtime argument value "dataplexOutputTable" for key "table"
    And Enter runtime argument value "dataplexTruncateTableTrue" for key "truncateTable"
    And Enter runtime argument value "dataplexUpdateSchemaTrue" for key "updateTableSchema"
    And Enter runtime argument value "dataplexPartitionFilterTrue" for key "partitionFilter"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count is equal to IN record count

  @BATCH-TS-DATAPLEX-MACRO-02
  Scenario: Verify the pipeline is deployed and pipeline preview is successful using macro parameters when the asset
  type is set to storage bucket.
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
    And Click on the Macro button of Property: "projectId" and set the value to: "project"
    And Click on the Macro button of Property: "dataplexLocationID" and set the value to: "location"
    And Click on the Macro button of Property: "dataplexLakeID" and set the value to: "lake"
    And Click on the Macro button of Property: "dataplexZoneID" and set the value to: "zone"
    And Click on the Macro button of Property: "dataplexAssetID" and set the value to: "asset"
    And Select radio button plugin property: "dataplexAssetType" with value: "dataplexStorageBucketRadioBtn"
    And Click on the Macro button of Property: "table" and set the value to: "table"
    And Click on the Macro button of Property: "formatMacroInput" and set the value to: "format"
    And Validate "Dataplex" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "projectId" for key "project"
    And Enter runtime argument value "dataplexLocation" for key "location"
    And Enter runtime argument value "dataplexLake" for key "lake"
    And Enter runtime argument value "dataplexZone" for key "zone"
    And Enter runtime argument value "dataplexAssetStorageBucket" for key "asset"
    And Enter runtime argument value "dataplexOutputTable" for key "table"
    And Enter runtime argument value "dataplexFormatAvro" for key "format"
    And Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    And Deploy the pipeline
    And Run the Pipeline in Runtime
    And Enter runtime argument value "projectId" for key "project"
    And Enter runtime argument value "dataplexLocation" for key "location"
    And Enter runtime argument value "dataplexLake" for key "lake"
    And Enter runtime argument value "dataplexZone" for key "zone"
    And Enter runtime argument value "dataplexAssetStorageBucket" for key "asset"
    And Enter runtime argument value "dataplexOutputTable" for key "table"
    And Enter runtime argument value "dataplexFormatAvro" for key "format"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count is equal to IN record count
