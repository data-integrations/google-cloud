@Dataplex_Source
Feature: Dataplex Source - Verify error Scenarios

  @BATCH-TS-DATAPLEX-Error-01
  Scenario: Verify dataplex source properties validation errors for mandatory fields
    When Open Datafusion Project to configure pipeline
    Then Select plugin: "Dataplex (Preview)" from the plugins list as: "Source"
    And Select Sink plugin: "Dataplex" from the plugins list
    And Connect plugins: "Dataplex (Preview)" and "Dataplex (Preview)2" to establish connection
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Click on the Validate button
    And Verify mandatory property error for below listed properties:
      | referenceName |
      | location      |
      | lake          |
      | zone          |
      | entity        |

  @BATCH-TS-DATAPLEX-Error-02
  Scenario: To verify user should not be able to get output schema when invalid data is used in the mandatory fields in
  dataplex source
    When Open Datafusion Project to configure pipeline
    Then Select plugin: "Dataplex (Preview)" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexInvalidLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexInvalidLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexInvalidZone"
    And Enter input plugin property: "dataplexEntityID" with value: "dataplexEntity"
    And Click on the Validate button
    And Verify plugin properties validation fails with 1 error

  @BATCH-TS-DATAPLEX-Error-03
  Scenario: Validate invalid entity error message by giving wrong entity
    When Open Datafusion Project to configure pipeline
    Then Select plugin: "Dataplex (Preview)" from the plugins list as: "Source"
    And Navigate to the properties page of plugin: "Dataplex (Preview)"
    And Enter input plugin property: "referenceName" with value: "dataplexReferenceName"
    And Replace input plugin property: "projectId" with value: "projectId"
    And Enter input plugin property: "dataplexLocationID" with value: "dataplexLocation"
    And Enter input plugin property: "dataplexLakeID" with value: "dataplexLake"
    And Enter input plugin property: "dataplexZoneID" with value: "dataplexZone"
    And Enter input plugin property: "dataplexEntityID" with value: "dataplexInvalidEntity"
    And Click on the Validate button
    And Verify that the Plugin Property: "entity" is displaying an in-line error message: "dataplexInvalidEntityError"
