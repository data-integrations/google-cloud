Feature: End to End records transfer from cloudSQLPostgreSQL to BigQuery and GCS

  @cloudSQLPostgreSQL-e2e
  Scenario:Verify all the records transfer from cloudSQLPostgreSQL to Bigquery supporting different data types
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    When Target is BigQuery
    Then Open cloudSQLPostgreSQL Properties
    Then Enter the cloudSQLPostgreSQL properties for database "cloudPsqlDbName" using query "cloudPSQLDBImportQueryForAll"
    Then Capture output schema
    Then Validate cloudSQLPostgreSQL properties
    Then Close the cloudSQLPostgreSQL properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery Target Properties for table "cloudPsqlBigQuery"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "CloudSQL-PostgreSQL" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for cloudSQLPostgreSQL
    Then Verify Preview output schema is not null
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Get Count of no of records transferred to BigQuery in "cloudPsqlBigQuery"
    Then Validate records out from cloudSQLPostgreSQL is equal to records transferred in BigQuery "cloudPsqlBigQuery" output records

  @cloudSQLPostgreSQL-e2e
  Scenario:Verify all the duplicate records are fetched and transferred to BigQuery
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    When Target is BigQuery
    Then Open cloudSQLPostgreSQL Properties
    Then Enter the cloudSQLPostgreSQL properties for database "cloudPsqlDbName" using query "cloudPSQLDBImportQueryDuplicate" for duplicate values "cloudPSQLSplitColumnDuplicateValue"
    Then Capture output schema
    Then Validate cloudSQLPostgreSQL properties
    Then Close the cloudSQLPostgreSQL properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery Target Properties for table "cloudPsqlBigQuery"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "CloudSQL-PostgreSQL" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for cloudSQLPostgreSQL
    Then Verify Preview output schema is not null
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Get Count of no of records transferred to BigQuery in "cloudPsqlBigQuery"
    Then Validate records out from cloudSQLPostgreSQL is equal to records transferred in BigQuery "cloudPsqlBigQuery" output records

  @cloudSQLPostgreSQL-e2e
  Scenario Outline: Verify records get transferred on combining different tables using joins
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    When Target is BigQuery
    Then Open cloudSQLPostgreSQL Properties
    Then Enter the cloudSQLPostgreSQL properties for database "cloudPsqlDbName" using different join queries "<cloudPSQLDBImportQueryJoins>"
    Then Capture output schema
    Then Validate cloudSQLPostgreSQL properties
    Then Close the cloudSQLPostgreSQL properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery Target Properties for table "cloudPsqlBigQuery"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "CloudSQL-PostgreSQL" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for cloudSQLPostgreSQL
    Then Verify Preview output schema is not null
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Get Count of no of records transferred to BigQuery in "cloudPsqlBigQuery"
    Then Validate records out from cloudSQLPostgreSQL is equal to records transferred in BigQuery "cloudPsqlBigQuery" output records
    Examples:
      |  cloudPSQLDBImportQueryJoins     |
      |  cloudPSQLDBImportQueryInnerJoin |
      |  cloudPSQLDBImportQueryLeftJoin  |
      |  cloudPSQLDBImportQueryRightJoin |
      |  cloudPSQLDBImportQueryOuterJoin |

  @cloudSQLPostgreSQL-e2e
  Scenario:Verify only distinct records are transferred
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    When Target is BigQuery
    Then Open cloudSQLPostgreSQL Properties
    Then Enter the cloudSQLPostgreSQL properties for database "cloudPsqlDbName" using query "cloudPSQLDBImportQueryDistinct" for distinct values "cloudPSQLSplitColumnDistinctValue"
    Then Capture output schema
    Then Validate cloudSQLPostgreSQL properties
    Then Close the cloudSQLPostgreSQL properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery Target Properties for table "cloudPsqlBigQuery"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "CloudSQL-PostgreSQL" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for cloudSQLPostgreSQL
    Then Verify Preview output schema is not null
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Get Count of no of records transferred to BigQuery in "cloudPsqlBigQuery"
    Then Validate records out from cloudSQLPostgreSQL is equal to records transferred in BigQuery "cloudPsqlBigQuery" output records

  @cloudSQLPostgreSQL-e2e
  Scenario:Verify records with maximum values are transferred from cloudPSQL to BigQuery
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    When Target is BigQuery
    Then Open cloudSQLPostgreSQL Properties
    Then Enter the cloudSQLPostgreSQL properties for database "cloudPsqlDbName" using query "cloudPSQLDBImportQueryForMax" for max values "cloudPSQLSplitColumnMaxValue"
    Then Capture output schema
    Then Validate cloudSQLPostgreSQL properties
    Then Close the cloudSQLPostgreSQL properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery Target Properties for table "cloudPsqlBigQuery"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "CloudSQL-PostgreSQL" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for cloudSQLPostgreSQL
    Then Verify Preview output schema is not null
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Get Count of no of records transferred to BigQuery in "cloudPsqlBigQuery"
    Then Validate records out from cloudSQLPostgreSQL is equal to records transferred in BigQuery "cloudPsqlBigQuery" output records

  @cloudSQLPostgreSQL-e2e
  Scenario:Verify records with minimum values are transferred from cloudPSQL to BigQuery
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    When Target is BigQuery
    Then Open cloudSQLPostgreSQL Properties
    Then Enter the cloudSQLPostgreSQL properties for database "cloudPsqlDbName" using query "cloudPSQLDBImportQueryForMin" for min values "cloudPSQLSplitColumnMinValue"
    Then Capture output schema
    Then Validate cloudSQLPostgreSQL properties
    Then Close the cloudSQLPostgreSQL properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery Target Properties for table "cloudPsqlBigQuery"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "CloudSQL-PostgreSQL" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for cloudSQLPostgreSQL
    Then Verify Preview output schema is not null
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Get Count of no of records transferred to BigQuery in "cloudPsqlBigQuery"
    Then Validate records out from cloudSQLPostgreSQL is equal to records transferred in BigQuery "cloudPsqlBigQuery" output records


  @cloudSQLPostgreSQL-e2e
  Scenario Outline: Verify all the records transfer from cloudSQLPostgreSQL to Bigquery for different where clause
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    When Target is BigQuery
    Then Open cloudSQLPostgreSQL Properties
    Then Enter the cloudSQLPostgreSQL properties for database "cloudPsqlDbName" using query "<cloudPostgresSQLDatabaseImportQuery>" for max and min "<cloudPostgresSQLSplitColumnValues>"
    Then Capture output schema
    Then Validate cloudSQLPostgreSQL properties
    Then Close the cloudSQLPostgreSQL properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery Target Properties for table "cloudPsqlBigQuery"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "CloudSQL-PostgreSQL" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for cloudSQLPostgreSQL
    Then Verify Preview output schema is not null
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Get Count of no of records transferred to BigQuery in "cloudPsqlBigQuery"
    Then Validate records out from cloudSQLPostgreSQL is equal to records transferred in BigQuery "cloudPsqlBigQuery" output records
    Examples:
      |  cloudPostgresSQLDatabaseImportQuery       |  cloudPostgresSQLSplitColumnValues         |
      |  cloudPSQLDBImportQueryForBetween          |   cloudPSQLSplitColumnBetweenValue         |
      |  cloudPSQLDBImportQueryForIn               |   cloudPSQLSplitColumnInValue              |
      |  cloudPSQLDBImportQueryNotIn               |   cloudPSQLSplitColumnNotInValue           |
      |  cloudPSQLDBImportQueryOrderBy             |   cloudPSQLSplitColumnOrderByValue         |


  @cloudSQLPostgreSQL-e2e
  Scenario:Verify records are transferred from cloudSQLPostgreSQL to BigQuery using Bounding Query
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    When Target is BigQuery
    Then Open cloudSQLPostgreSQL Properties
    Then Enter the cloudSQLPostgreSQL properties for database "cloudPsqlDbName" using query "cloudPSQLQuery" for max values "cloudPSQLSplitColumnMaxValue" with bounding query "cloudPSQLDBBoundingQuery" and "cloudPsqlNoOfSplits"
    Then Capture output schema
    Then Validate cloudSQLPostgreSQL properties
    Then Close the cloudSQLPostgreSQL properties
    Then Open BigQuery Target Properties
    Then Enter the BigQuery Target Properties for table "cloudPsqlBigQuery"
    Then Validate Bigquery properties
    Then Close the BigQuery properties
    Then Connect Source as "CloudSQL-PostgreSQL" and sink as "BigQuery" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for cloudSQLPostgreSQL
    Then Verify Preview output schema is not null
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Get Count of no of records transferred to BigQuery in "cloudPsqlBigQuery"
    Then Validate records out from cloudSQLPostgreSQL is equal to records transferred in BigQuery "cloudPsqlBigQuery" output records

  @cloudSQLPostgreSQL-e2e
  Scenario:Verify all the records transfer from cloudSQLPostgreSQL to GCS supporting different data types
    Given Open DataFusion Project to configure pipeline
    When Source is CloudSQLPostGreSQL
    When Sink is GCS
    Then Open cloudSQLPostgreSQL Properties
    Then Enter the cloudSQLPostgreSQL properties for database "cloudPsqlDbName" using query "cloudPSQLDBImportQueryForAll"
    Then Capture output schema
    Then Validate cloudSQLPostgreSQL properties
    Then Close the cloudSQLPostgreSQL properties
    Then Enter the GCS Properties
    Then Close the GCS Properties
    Then Connect Source as "CloudSQL-PostgreSQL" and sink as "GCS" to establish connection
    Then Add pipeline name
    Then Preview and run the pipeline
    Then Verify the preview of pipeline is "success"
    Then Click on PreviewData for cloudSQLPostgreSQL
    Then Verify Preview output schema is not null
    Then Close the Preview and deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Open the Logs and capture raw logs
    Then Validate the output record count
