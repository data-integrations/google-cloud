package io.cdap.plugin.gcp.bigquery.sqlengine;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import io.cdap.plugin.gcp.bigquery.sqlengine.util.BigQuerySQLEngineUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * SQL Dataset that represents the result of a "Select" operation, such as join, that is executed in BigQuery.
 */
public class BigQuerySelectDataset implements SQLDataset, BigQuerySQLDataset {

  private static final Logger LOG = LoggerFactory.getLogger(BigQuerySelectDataset.class);

  private final String datasetName;
  private final Schema outputSchema;
  private final BigQuerySQLEngineConfig sqlEngineConfig;
  private final BigQuery bigQuery;
  private final String project;
  private final String bqDataset;
  private final String bqTable;
  private final String jobId;
  private final String operation;
  private final String selectQuery;
  private Long numRows;

  protected BigQuerySelectDataset(String datasetName,
      Schema outputSchema,
      BigQuerySQLEngineConfig sqlEngineConfig,
      BigQuery bigQuery,
      String project,
      String bqDataset,
      String bqTable,
      String jobId,
      String operation,
      String selectQuery) {
    this.datasetName = datasetName;
    this.outputSchema = outputSchema;
    this.sqlEngineConfig = sqlEngineConfig;
    this.bigQuery = bigQuery;
    this.project = project;
    this.bqDataset = bqDataset;
    this.bqTable = bqTable;
    this.jobId = jobId;
    this.operation = operation;
    this.selectQuery = selectQuery;
  }

  public BigQuerySelectDataset execute() {
    TableId destinationTable = TableId.of(project, bqDataset, bqTable);

    // Get location for target dataset. This way, the job will run in the same location as the dataset
    DatasetId destinationDataset = DatasetId.of(project, bqDataset);
    Dataset dataset = bigQuery.getDataset(destinationDataset);
    String location = dataset.getLocation();

    LOG.info("Creating table `{}` using job: {} with SQL statement: {}", bqTable, jobId,
        selectQuery);

    // Run BigQuery job with generated SQL statement, store results in a new table, and set priority to BATCH
    // TODO: Make priority configurable
    QueryJobConfiguration queryConfig =
        QueryJobConfiguration.newBuilder(selectQuery)
            .setDestinationTable(destinationTable)
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_NEVER)
            .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
            .setPriority(sqlEngineConfig.getJobPriority())
            .setLabels(BigQuerySQLEngineUtils.getJobTags(operation))
            .build();

    // Create a job ID so that we can safely retry.
    JobId bqJobId = JobId.newBuilder().setJob(jobId).setLocation(location).setProject(project).build();
    Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(bqJobId).build());

    // Wait for the query to complete.
    try {
      queryJob = queryJob.waitFor();
    } catch (InterruptedException ie) {
      throw new SQLEngineException("Interrupted exception when executing Join operation", ie);
    }

    // Check for errors
    if (queryJob == null) {
      throw new SQLEngineException("BigQuery job not found: " + jobId);
    } else if (queryJob.getStatus().getError() != null) {
      throw new SQLEngineException(String.format(
          "Error executing BigQuery Job: '%s' in Project '%s', Dataset '%s', Location'%s' : %s",
          jobId, project, bqDataset, location, queryJob.getStatus().getError().toString()));
    }

    LOG.info("Created BigQuery table `{}` using Job: {}", bqTable, jobId);
    return this;
  }

  @Override
  public String getDatasetName() {
    return datasetName;
  }

  @Override
  public Schema getSchema() {
    return outputSchema;
  }

  @Override
  public long getNumRows() {
    // Get the number of rows from BQ if not known at this time.
    if (numRows == null) {
      numRows = BigQuerySQLEngineUtils.getNumRows(bigQuery, project, bqDataset, bqTable);
    }

    return numRows;
  }

  @Override
  public String getBigQueryTableName() {
    return bqTable;
  }

  @Override
  @Nullable
  public String getGCSPath() {
    return null;
  }

  @Override
  public String getJobId() {
    return jobId;
  }
}
