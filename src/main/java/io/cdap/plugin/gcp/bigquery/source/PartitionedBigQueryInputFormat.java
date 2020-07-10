package io.cdap.plugin.gcp.bigquery.source;

import com.google.api.services.bigquery.model.Job;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.JobReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableDefinition.Type;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.hadoop.io.bigquery.AbstractBigQueryInputFormat;
import com.google.cloud.hadoop.io.bigquery.AvroBigQueryInputFormat;
import com.google.cloud.hadoop.io.bigquery.AvroRecordReader;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.cloud.hadoop.io.bigquery.BigQueryUtils;
import com.google.cloud.hadoop.io.bigquery.ExportFileFormat;
import com.google.cloud.hadoop.util.ConfigurationUtil;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.sun.istack.Nullable;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * BigQuery input format, splits query from the configuration into list of queries
 * in order to create input splits.
 */
public class PartitionedBigQueryInputFormat extends AbstractBigQueryInputFormat<LongWritable, GenericData.Record> {
  private static final String DEFAULT_COLUMN_NAME = "_PARTITIONTIME";

  private InputFormat<LongWritable, GenericData.Record> delegateInputFormat =
    new AvroBigQueryInputFormat();

  @Override
  public ExportFileFormat getExportFileFormat() {
    return ExportFileFormat.AVRO;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    processQuery(context);

    return delegateInputFormat.getSplits(context);
  }


  @Override
  public RecordReader<LongWritable, GenericData.Record> createDelegateRecordReader(InputSplit split,
                                                                                   Configuration configuration)
    throws IOException, InterruptedException {
    Preconditions.checkState(
      split instanceof FileSplit, "AvroBigQueryInputFormat requires FileSplit input splits");
    return new AvroRecordReader();
  }

  private void processQuery(JobContext context) throws IOException, InterruptedException {
    final Configuration configuration = context.getConfiguration();
    BigQueryHelper bigQueryHelper;
    try {
      bigQueryHelper = getBigQueryHelper(configuration);
    } catch (GeneralSecurityException gse) {
      throw new IOException("Failed to create BigQuery client", gse);
    }
    Map<String, String> mandatoryConfig = ConfigurationUtil.getMandatoryConfig(
      configuration, BigQueryConfiguration.MANDATORY_CONFIG_PROPERTIES_INPUT);
    String inputProjectId = mandatoryConfig.get(BigQueryConfiguration.INPUT_PROJECT_ID_KEY);
    String datasetId = mandatoryConfig.get(BigQueryConfiguration.INPUT_DATASET_ID_KEY);
    String tableName = mandatoryConfig.get(BigQueryConfiguration.INPUT_TABLE_ID_KEY);
    String serviceFilePath = configuration.get(BigQueryConstants.CONFIG_SERVICE_ACCOUNT_FILE_PATH, null);

    String partitionFromDate = configuration.get(BigQueryConstants.CONFIG_PARTITION_FROM_DATE, null);
    String partitionToDate = configuration.get(BigQueryConstants.CONFIG_PARTITION_TO_DATE, null);
    String filter = configuration.get(BigQueryConstants.CONFIG_FILTER, null);

    com.google.cloud.bigquery.Table bigQueryTable =
        BigQueryUtil.getBigQueryTable(inputProjectId, datasetId, tableName, serviceFilePath);
    Type type = Objects.requireNonNull(bigQueryTable).getDefinition().getType();

    String query;
    if (type == Type.VIEW) {
      query = generateQueryForMaterializingView(datasetId, tableName, filter);
    } else {
      query = generateQuery(partitionFromDate, partitionToDate, filter, inputProjectId, datasetId, tableName,
          serviceFilePath);
    }


    if (query != null) {
      TableReference sourceTable = new TableReference()
        .setDatasetId(datasetId)
        .setProjectId(inputProjectId)
        .setTableId(tableName);

      String location = bigQueryHelper.getTable(sourceTable).getLocation();
      String temporaryTableName = String.format("%s_%s", tableName, UUID.randomUUID().toString().replaceAll("-", "_"));
      TableReference exportTableReference = createExportTableReference(type, inputProjectId, datasetId,
          temporaryTableName, configuration);

      runQuery(bigQueryHelper, inputProjectId, exportTableReference, query, location);
      if (type == Type.VIEW) {
        configuration.set(
            BigQueryConfiguration.INPUT_PROJECT_ID_KEY,
            configuration.get(BigQueryConstants.CONFIG_VIEW_MATERIALIZATION_PROJECT));
        configuration.set(
            BigQueryConfiguration.INPUT_DATASET_ID_KEY,
            configuration.get(BigQueryConstants.CONFIG_VIEW_MATERIALIZATION_DATASET));
      }
      configuration.set(BigQueryConfiguration.INPUT_TABLE_ID_KEY, temporaryTableName);
    }
  }

  private String generateQuery(String partitionFromDate, String partitionToDate, String filter,
                               String project, String dataset, String table, @Nullable String serviceFilePath) {
    if (partitionFromDate == null && partitionToDate == null && filter == null) {
      return null;
    }
    String queryTemplate = "select * from %s where %s";
    com.google.cloud.bigquery.Table sourceTable = BigQueryUtil.getBigQueryTable(project, dataset, table,
                                                                                serviceFilePath);
    StandardTableDefinition tableDefinition = Objects.requireNonNull(sourceTable).getDefinition();
    TimePartitioning timePartitioning = tableDefinition.getTimePartitioning();
    if (timePartitioning == null && filter == null) {
      return null;
    }
    StringBuilder condition = new StringBuilder();

    if (timePartitioning != null) {
      String timePartitionCondition = generateTimePartitionCondition(tableDefinition, timePartitioning,
                                                                     partitionFromDate, partitionToDate);
      condition.append(timePartitionCondition);
    }

    if (filter != null) {
      if (condition.length() == 0) {
        condition.append(filter);
      } else {
        condition.append(" and (").append(filter).append(")");
      }
    }

    String tableName = dataset + "." + table;
    return String.format(queryTemplate, tableName, condition.toString());
  }

  private String generateQueryForMaterializingView(String dataset, String table, String filter) {
    String queryTemplate = "select * from %s %s";
    StringBuilder condition = new StringBuilder();

    if (!Strings.isNullOrEmpty(filter)) {
      condition.append(String.format(" where %s", filter));
    }

    String tableName = dataset + "." + table;
    return String.format(queryTemplate, tableName, condition.toString());
  }


  /**
   * Create {@link TableReference} to export Table or View
   *
   * @param type BigQuery table type
   * @param inputProjectId project id of source table
   * @param datasetId dataset id of source table
   * @param tableId The ID of the table
   * @param configuration Configuration that contains View Materialization ProjectId and View
   *     Materialization Dataset Id
   * @return {@link TableReference}
   */
  private TableReference createExportTableReference(
      Type type,
      String inputProjectId,
      String datasetId,
      String tableId,
      Configuration configuration) {

    TableReference tableReference = new TableReference().setTableId(tableId);

    if (type == Type.VIEW) {
      tableReference.setProjectId(configuration.get(BigQueryConstants.CONFIG_VIEW_MATERIALIZATION_PROJECT));
      tableReference.setDatasetId(configuration.get(BigQueryConstants.CONFIG_VIEW_MATERIALIZATION_DATASET));
    } else {
      tableReference.setProjectId(inputProjectId);
      tableReference.setDatasetId(datasetId);
    }

    return tableReference;
  }

  private static void runQuery(
    BigQueryHelper bigQueryHelper, String projectId, TableReference tableRef, String query, String location)
    throws IOException, InterruptedException {

    // Create a query statement and query request object.
    JobConfigurationQuery queryConfig = new JobConfigurationQuery();
    queryConfig.setAllowLargeResults(true);
    queryConfig.setQuery(query);
    queryConfig.setUseLegacySql(false);

    // Set the table to put results into.
    queryConfig.setDestinationTable(tableRef);

    queryConfig.setCreateDisposition("CREATE_IF_NEEDED");

    // Require table to be empty.
    queryConfig.setWriteDisposition("WRITE_EMPTY");

    JobConfiguration config = new JobConfiguration();
    config.setQuery(queryConfig);

    JobReference jobReference =
      bigQueryHelper.createJobReference(projectId, "querybasedexport", location);

    Job job = new Job();
    job.setConfiguration(config);
    job.setJobReference(jobReference);

    // Run the job.
    Job response = bigQueryHelper.insertJobOrFetchDuplicate(projectId, job);

    // Create anonymous Progressable object
    Progressable progressable = new Progressable() {
      @Override
      public void progress() {
        // TODO(user): ensure task doesn't time out
      }
    };

    // Poll until job is complete.
    BigQueryUtils.waitForJobCompletion(
      bigQueryHelper.getRawBigquery(), projectId, jobReference, progressable);
    if (bigQueryHelper.tableExists(tableRef)) {
      long expirationMillis = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1);
      Table table = bigQueryHelper.getTable(tableRef).setExpirationTime(expirationMillis);
      bigQueryHelper.getRawBigquery().tables().update(tableRef.getProjectId(), tableRef.getDatasetId(),
                                                      tableRef.getTableId(), table).execute();
    }
  }

  private String generateTimePartitionCondition(StandardTableDefinition tableDefinition,
                                                TimePartitioning timePartitioning, String partitionFromDate,
                                                String partitionToDate) {
    StringBuilder timePartitionCondition = new StringBuilder();
    String columnName = timePartitioning.getField() != null ? timePartitioning.getField() : DEFAULT_COLUMN_NAME;

    LegacySQLTypeName columnType = null;
    if (!DEFAULT_COLUMN_NAME.equals(columnName)) {
      columnType = tableDefinition.getSchema().getFields().get(columnName).getType();
    }

    if (partitionFromDate != null) {
      if (LegacySQLTypeName.DATE.equals(columnType)) {
        columnName = "TIMESTAMP(\"" + columnName + "\")";
      }
      timePartitionCondition.append(columnName).append(" >= ").append("TIMESTAMP(\"")
        .append(partitionFromDate).append("\")");
    }
    if (partitionFromDate != null && partitionToDate != null) {
      timePartitionCondition.append(" and ");
    }
    if (partitionToDate != null) {
      if (LegacySQLTypeName.DATE.equals(columnType)) {
        columnName = "TIMESTAMP(\"" + columnName + "\")";
      }
      timePartitionCondition.append(columnName).append(" < ").append("TIMESTAMP(\"")
        .append(partitionToDate).append("\")");
    }
    return timePartitionCondition.toString();
  }
}
