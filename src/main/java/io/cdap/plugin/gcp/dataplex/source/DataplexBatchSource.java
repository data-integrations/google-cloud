package io.cdap.plugin.gcp.dataplex.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.gcp.dataplex.sink.connection.DataplexInterface;
import io.cdap.plugin.gcp.dataplex.sink.connection.out.DataplexInterfaceImpl;
import io.cdap.plugin.gcp.dataplex.source.config.DataplexBatchSourceConfig;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.LongWritable;

/**
 * Dataplex Batch Source Plugin
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(DataplexBatchSource.NAME)
@Description("Dataplex Source")
public class DataplexBatchSource extends BatchSource<LongWritable, GenericData.Record, StructuredRecord> {
  public static final String NAME = "Dataplex";
  public static final String BIGQUERY_DATASET_ASSET_TYPE = "BIGQUERY_DATASET";
  public static final String STORAGE_BUCKET_ASSET_TYPE = "STORAGE_BUCKET";
  private final DataplexBatchSourceConfig config;

  public DataplexBatchSource(DataplexBatchSourceConfig dataplexBatchSourceConfig) {
    this.config = dataplexBatchSourceConfig;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    if (!config.getConnection().canConnect() || config.getServiceAccountType() == null ||
      (config.isServiceAccountFilePath() && config.autoServiceAccountUnavailable()) ||
      (config.tryGetProject() == null)) {
      return;
    }

    StageConfigurer configurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = configurer.getFailureCollector();
    config.validateServiceAccount(collector);
    DataplexInterface dataplexInterface = new DataplexInterfaceImpl();
    config.validateAssetConfiguration(collector, dataplexInterface);
    if (config.getAssetType().equals(BIGQUERY_DATASET_ASSET_TYPE)) {
      config.validateBigQueryDataset(collector);
      config.validateTable(collector);
    } else if (config.getAsset().equals(STORAGE_BUCKET_ASSET_TYPE)) {
      config.validateGCS(collector);
    }

  }

  @Override
  public void prepareRun(BatchSourceContext batchSourceContext) {
    // prepare run
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }
}
