@Dataplex_Source
Feature: Dataplex Source - Verification of pipeline preview and deployment using macros

  @BATCH-TS-DATAPLEX-MACRO-01
  Scenario: With Storage bucket as asset Verify the preview and deployment is succesful from Dataplex to Dataplex with macro arguments
    When Open Datafusion Project to configure pipeline
    Then Select plugin: "Dataplex (Preview)" from the plugins list as: "Source"
    And Select Sink plugin: "Dataplex" from the plugins list
    And Connect plugins: "Dataplex (Preview)" and "Dataplex (Preview)2" to establish connection
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Click on the Macro button of Property: "projectId" and set the value to: "project"
    And Click on the Macro button of Property: "dataplexLocationID" and set the value to: "location"
    And Click on the Macro button of Property: "dataplexLakeID" and set the value to: "lake"
    And Click on the Macro button of Property: "dataplexZoneID" and set the value to: "zone"
    And Click on the Macro button of Property: "dataplexEntityID" and set the value to: "entity"
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
    And Select dropdown plugin property: "format" with option value: "dataplexFormatAvro"
    And Validate "Dataplex" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "projectId" for key "project"
    And Enter runtime argument value "dataplexLocation" for key "location"
    And Enter runtime argument value "dataplexLake" for key "lake"
    And Enter runtime argument value "dataplexZone" for key "zone"
    And Enter runtime argument value "dataplexEntity" for key "entity"
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
    And Enter runtime argument value "dataplexEntity" for key "entity"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count is equal to IN record count

  @BATCH-TS-DATAPLEX-MACRO-02
  Scenario: With BQ asset Verify the pipeline preview data and deployment is succesful from Dataplex to Dataplex with macro arguments
    When Open Datafusion Project to configure pipeline
    Then Select plugin: "Dataplex (Preview)" from the plugins list as: "Source"
    And Select Sink plugin: "Dataplex" from the plugins list
    And Connect plugins: "Dataplex (Preview)" and "Dataplex (Preview)2" to establish connection
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Click on the Macro button of Property: "projectId" and set the value to: "project"
    And Click on the Macro button of Property: "dataplexLocationID" and set the value to: "location"
    And Click on the Macro button of Property: "dataplexLakeID" and set the value to: "lake"
    And Click on the Macro button of Property: "dataplexZoneID" and set the value to: "zone"
    And Click on the Macro button of Property: "dataplexEntityID" and set the value to: "entity"
    And Validate "Dataplex" plugin properties
    And Close the Plugin Properties page
    And Navigate to the properties page of plugin: "Dataplex (Preview)2"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexAssetID" with value: "dataplexAssetBQ"
    And Enter input plugin property: "table" with value: "dataplexMacrosOutputTable"
    And Click plugin property: "truncateTable"
    And Click plugin property: "updateTableSchema"
    And Validate "Dataplex" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    And Enter runtime argument value "projectId" for key "project"
    And Enter runtime argument value "dataplexLocation" for key "location"
    And Enter runtime argument value "dataplexLake" for key "lake"
    And Enter runtime argument value "dataplexZone" for key "zone"
    And Enter runtime argument value "dataplexMacroSourceEntity" for key "entity"
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
    And Enter runtime argument value "dataplexMacroSourceEntity" for key "entity"
    And Run the Pipeline in Runtime with runtime arguments
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count is equal to IN record count
