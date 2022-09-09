@Dataplex_Source
Feature: Dataplex Source and Sink - Run time scenarios

  @BATCH-TS-DATAPLEX-RunTime-01 @Dataplex_Source_Required
  Scenario: Verify the pipeline preview data and pipeline deployment is successful when asset type is selected as BQ dataset
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
    And Save the pipeline
    And Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count

  @BATCH-TS-DATAPLEX-RunTime-02 @Dataplex_Source_Required
  Scenario Outline: To verify the pipelines is successfully deployed and run for different format when asset type is
  storage bucket
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
    And Enter input plugin property: "dataplexAssetID" with value: "dataplexAssetStorageBucket"
    And Select radio button plugin property: "dataplexAssetType" with value: "dataplexStorageBucketRadioBtn"
    And Enter input plugin property: "table" with value: "dataplexOutputTable"
    And Select dropdown plugin property: "format" with option value: "<Format>"
    And Validate "Dataplex" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Examples:
      | Format  |
      | avro    |
      | json    |
      | parquet |

   # orc and csv formats have been handled separately because some datatypes are not supported by them.
   # For e.g. record datatype is not supported by orc and csv.
   # The entity table, which is used by all other formats, contains these datatypes.
   # As a result, this "record" datatype has been removed from the entity table used for orc and csv formats.
  @BATCH-TS-DATAPLEX-RunTime-03
  Scenario Outline: To verify pipelines with csv and orc formats are successfully deployed when asset type is storage bucket
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
    And Enter input plugin property: "dataplexEntityID" with value: "dataplexOrcEntity"
    And Validate output schema with expectedSchema "dataplexOrcEntitySchema"
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
    And Enter input plugin property: "table" with value: "dataplexOrcOutputTable"
    And Select dropdown plugin property: "format" with option value: "<Format>"
    And Validate "Dataplex" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Examples:
      | Format |
      | csv    |
      | orc    |

  @BATCH-TS-DATAPLEX-RunTime-04
  Scenario: Verify the dataplex plugin data transfer with auto-detect project id
    When Open Datafusion Project to configure pipeline
    Then Select plugin: "Dataplex (Preview)" from the plugins list as: "Source"
    And Select Sink plugin: "Dataplex" from the plugins list
    And Connect plugins: "Dataplex (Preview)" and "Dataplex (Preview)2" to establish connection
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexEntityID" with value: "dataplexEntity"
    And Validate output schema with expectedSchema "dataplexSourceSchema"
    And Validate "Dataplex" plugin properties
    And Close the Plugin Properties page
    And Navigate to the properties page of plugin: "Dataplex (Preview)2"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexAssetID" with value: "dataplexAssetBQ"
    And Enter input plugin property: "table" with value: "dataplexOutputTable"
    And Validate "Dataplex" plugin properties
    And Close the Plugin Properties page
    And Save the pipeline
    And Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait till pipeline is in running state
    And Open and capture logs
    And Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
