@GCS_Source
Feature: GCS source - Verification of GCS to BQ successful data transfer

  @BQ_SINK_TEST @GCS_READ_RECURSIVE_TEST
  Scenario: To verify data is getting transferred from GCS to BigQuery using read file recursive functionality
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsReadRecursivePath"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Select GCS source property read file recursive as "true"
    Then Validate output schema with expectedSchema "gcsReadRecursivePathSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on preview data for BigQuery sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table

  @BQ_SINK_TEST @GCS_DATATYPE_TEST
  Scenario: To verify data is getting transferred from GCS to BigQuery with supported DataTypes
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsDataTypeTestFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Click on the Get Schema button
    Then Change datatype of fields in output schema with : "gcsDatatypeChange"
    Then Validate "GCS" plugin properties
    Then Verify the Output Schema matches the Expected Schema: "gcsDataTypeTestFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table

  @BQ_SINK_TEST @GCS_DATATYPE_2_TEST
  Scenario: To verify data is getting transferred from GCS to BigQuery with special cases of float DataTypes
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsDataTypeTest2File"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsDataTypeTest2FileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table

  @GCS_OUTPUT_FIELD_TEST @BQ_SINK_TEST
  Scenario: To verify successful data transfer from GCS to BigQuery with outputField and path filename only
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsOutputFieldTestFile"
    Then Select GCS property format "csv"
    Then Enter GCS source property path field "gcsPathField"
    Then Select GCS source property path filename only as "true"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "gcsOutputFieldTestFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Then Verify output field "gcsPathField" in target BigQuery table contains filename of the source GcsBucket "gcsOutputFieldTestFile"

  @GCS_DELIMITED_TEST @BQ_SINK_TEST
  Scenario: To verify data is getting transferred from GCS to BigQuery with File system properties
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsDelimitedFile"
    Then Select GCS property format "text"
    Then Enter GCS File system properties field "gcsFileSysProperty"
    Then Click on Tidy in GCS File system properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table

  @GCS_CSV_TEST @BQ_SINK_TEST
  Scenario Outline: To verify data is getting transferred from GCS to BigQuery with different encoding types
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsCsvFile"
    Then Select GCS property format "csv"
    Then Toggle GCS source property skip header to true
    Then Select GCS source property file encoding type "<EncodingType>"
    Then Validate output schema with expectedSchema "gcsCsvFileSchema"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
    Examples:
      | EncodingType |
      | IBM00858     |
      | Windows-1250 |
      | ISO-8859-1   |
    @PLUGIN-823
    Examples:
      | EncodingType |
      | UTF-32       |

  @BQ_SINK_TEST @GCS_DATATYPE_1_TEST
  Scenario Outline: To verify GCS Source output schema validation for various combinations of Datatype override
    Given Open Datafusion Project to configure pipeline
    When Source is GCS
    When Sink is BigQuery
    Then Connect source as "GCS" and sink as "BigQuery" to establish connection
    Then Open GCS source properties
    Then Enter GCS property projectId and reference name
    Then Enter GCS source property path "gcsDataTypeTest1File"
    Then Select GCS property format "csv"
    Then Enter GCS source property override field "<Column>" and data type "<DataType>"
    Then Toggle GCS source property skip header to true
    Then Validate output schema with expectedSchema "<ExpectedSchema>"
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Open BigQuery sink properties
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Examples:
      | Column            |  DataType                    | ExpectedSchema                    |
      | gcsOverrideField  | gcsOverrideFloatDataType     | gcsOverrideInt_FloatSchema        |
      | gcsOverrideField2 | gcsOverrideLongDataType      | gcsOverrideInt_LongSchema         |
      | gcsOverrideField4 | gcsOverrideFloatDataType     | gcsOverrideString_FloatSchema     |
      | gcsOverrideField6 | gcsOverrideDoubleDataType    | gcsOverrideString_DoubleSchema    |
      | gcsOverrideField7 | gcsOverrideFloatDataType     | gcsOverrideDouble_FloatSchema     |
    @PLUGIN-1113
    Examples:
      | Column            | DataType                     | ExpectedSchema                    |
      | gcsOverrideField3 | gcsOverrideTimestampDataType | gcsOverrideString_TimestampSchema |
      | gcsOverrideField5 | gcsOverrideDateDataType      | gcsOverrideString_DateSchema      |
