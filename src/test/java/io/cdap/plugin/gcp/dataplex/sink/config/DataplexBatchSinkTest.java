package io.cdap.plugin.gcp.dataplex.sink.config;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.dataplex.v1.Asset;
import com.google.cloud.dataplex.v1.AssetName;
import com.google.cloud.dataplex.v1.DataplexServiceClient;
import com.google.cloud.dataplex.v1.Entity;
import com.google.cloud.dataplex.v1.Lake;
import com.google.cloud.dataplex.v1.LakeName;
import com.google.cloud.dataplex.v1.MetadataServiceClient;
import com.google.cloud.dataplex.v1.Schema;
import com.google.cloud.dataplex.v1.Zone;
import com.google.cloud.dataplex.v1.ZoneName;
import com.google.cloud.kms.v1.CryptoKeyName;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkUtils;
import io.cdap.plugin.gcp.bigquery.sink.lib.BigQueryTableFieldSchema;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.CmekUtils;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexConstants;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexUtil;
import io.cdap.plugin.gcp.dataplex.sink.DataplexBatchSink;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(fullyQualifiedNames = {"io.cdap.plugin.gcp.dataplex.*", "io.cdap.plugin.gcp.bigquery.*",
  "io.cdap.plugin.gcp.common.*"})
public class DataplexBatchSinkTest {

  @Test
  public void testConfigurePipeline() throws IOException {
    PipelineConfigurer pipelineConfigurer = Mockito.mock(PipelineConfigurer.class);
    StageConfigurer configurer = Mockito.mock(StageConfigurer.class);
    Mockito.when(pipelineConfigurer.getStageConfigurer()).thenReturn(configurer);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(configurer.getFailureCollector()).thenReturn(mockFailureCollector);
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.doNothing().when(dataplexServiceClient).close();
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    when(dataplexConnectorConfig.getServiceAccountType()).thenReturn("account");
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      Mockito.spy(DataplexBatchSinkConfig.builder()
        .setReferenceName("test").setAssetType(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE).
        setConnection(dataplexConnectorConfig).setUpdateDataplexMetadata(true).build());
    when(dataplexBatchSinkConfig.getCredentials(mockFailureCollector)).thenReturn(googleCredentials);
    when(dataplexBatchSinkConfig.getConnection().canConnect()).thenReturn(true);
    DataplexBatchSink dataplexBatchSink = new DataplexBatchSink(dataplexBatchSinkConfig);
    StructuredRecord input = Mockito.mock(StructuredRecord.class);
    Emitter<KeyValue<Object, Object>> emitter = Mockito.mock(Emitter.class);
    dataplexBatchSink.transform(input, emitter);
    dataplexBatchSink.configurePipeline(pipelineConfigurer);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }


  @Test
  public void testConfigurePipelineWithAvroOutputFormat() throws IOException, NoSuchFieldException {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));
    PipelineConfigurer pipelineConfigurer = Mockito.mock(PipelineConfigurer.class);
    StageConfigurer configurer = Mockito.mock(StageConfigurer.class);
    Mockito.when(pipelineConfigurer.getStageConfigurer()).thenReturn(configurer);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(configurer.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(configurer.getInputSchema()).thenReturn(schema);
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    MetadataServiceClient metadataServiceClient = mock(MetadataServiceClient.class);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.when(DataplexUtil.getMetadataServiceClient(googleCredentials)).thenReturn(metadataServiceClient);
    Mockito.doNothing().when(dataplexServiceClient).close();
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    when(dataplexConnectorConfig.getServiceAccountType()).thenReturn("account");
    when(dataplexConnectorConfig.tryGetProject()).thenReturn("project");
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      Mockito.spy(DataplexBatchSinkConfig.builder()
        .setReferenceName("test").setAssetType(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE).
        setConnection(dataplexConnectorConfig).setUpdateDataplexMetadata(true).setTable("table").
        setFormat("avro").
        setLake("lake").setZone("zone").setLocation("location").build());
    when(dataplexBatchSinkConfig.getCredentials(mockFailureCollector)).thenReturn(googleCredentials);
    when(dataplexBatchSinkConfig.getConnection().canConnect()).thenReturn(true);
    when(dataplexBatchSinkConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    DataplexBatchSink dataplexBatchSink = new DataplexBatchSink(dataplexBatchSinkConfig);
    StructuredRecord input = Mockito.mock(StructuredRecord.class);
    Emitter<KeyValue<Object, Object>> emitter = Mockito.mock(Emitter.class);
    dataplexBatchSink.transform(input, emitter);
    dataplexBatchSink.configurePipeline(pipelineConfigurer);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }


  @Test
  public void testConfigurePipelineWithformatMacro() throws IOException, NoSuchFieldException {
    Set<String> macroFields = new HashSet<>();
    macroFields.add(DataplexBatchSinkConfig.NAME_FORMAT);
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("ts",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));
    PipelineConfigurer pipelineConfigurer = Mockito.mock(PipelineConfigurer.class);
    StageConfigurer configurer = Mockito.mock(StageConfigurer.class);
    Mockito.when(pipelineConfigurer.getStageConfigurer()).thenReturn(configurer);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(configurer.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(configurer.getInputSchema()).thenReturn(schema);
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    MetadataServiceClient metadataServiceClient = mock(MetadataServiceClient.class);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.when(DataplexUtil.getMetadataServiceClient(googleCredentials)).thenReturn(metadataServiceClient);
    Mockito.doNothing().when(dataplexServiceClient).close();
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    when(dataplexConnectorConfig.getServiceAccountType()).thenReturn("account");
    when(dataplexConnectorConfig.tryGetProject()).thenReturn("project");
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      Mockito.spy(DataplexBatchSinkConfig.builder()
        .setReferenceName("test").setAssetType(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE).
        setConnection(dataplexConnectorConfig).setUpdateDataplexMetadata(true).setTable("table").
        setLake("lake").setZone("zone").setLocation("location").build());
    //Setting the macro fields
    FieldSetter.setField(dataplexBatchSinkConfig, PluginConfig.class.getDeclaredField("macroFields"), macroFields);

    when(dataplexBatchSinkConfig.getCredentials(mockFailureCollector)).thenReturn(googleCredentials);
    when(dataplexBatchSinkConfig.getConnection().canConnect()).thenReturn(true);
    when(dataplexBatchSinkConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    DataplexBatchSink dataplexBatchSink = new DataplexBatchSink(dataplexBatchSinkConfig);
    StructuredRecord input = Mockito.mock(StructuredRecord.class);
    Emitter<KeyValue<Object, Object>> emitter = Mockito.mock(Emitter.class);
    dataplexBatchSink.transform(input, emitter);
    dataplexBatchSink.configurePipeline(pipelineConfigurer);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testConfigurePipelineWithBigQueryAssetType() throws IOException {
    PipelineConfigurer pipelineConfigurer = Mockito.mock(PipelineConfigurer.class);
    StageConfigurer configurer = Mockito.mock(StageConfigurer.class);
    Mockito.when(pipelineConfigurer.getStageConfigurer()).thenReturn(configurer);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(configurer.getFailureCollector()).thenReturn(mockFailureCollector);
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.doNothing().when(dataplexServiceClient).close();
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    when(dataplexConnectorConfig.getServiceAccountType()).thenReturn("account");
    when(dataplexConnectorConfig.tryGetProject()).thenReturn("project");
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      Mockito.spy(DataplexBatchSinkConfig.builder()
        .setReferenceName("test").setAssetType(DataplexConstants.BIGQUERY_DATASET_ASSET_TYPE).
        setConnection(dataplexConnectorConfig).setUpdateDataplexMetadata(true).setTable("table").
        setFormat("format").build());
    when(dataplexBatchSinkConfig.getCredentials(mockFailureCollector)).thenReturn(googleCredentials);
    when(dataplexBatchSinkConfig.getConnection().canConnect()).thenReturn(true);
    DataplexBatchSink dataplexBatchSink = new DataplexBatchSink(dataplexBatchSinkConfig);
    StructuredRecord input = Mockito.mock(StructuredRecord.class);
    Emitter<KeyValue<Object, Object>> emitter = Mockito.mock(Emitter.class);
    dataplexBatchSink.transform(input, emitter);
    dataplexBatchSink.configurePipeline(pipelineConfigurer);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testPrepareRun() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("field",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.DATE)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("Order",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.LogicalType.DATE)));
    com.google.cloud.bigquery.Schema bigQuerySchema = com.google.cloud.bigquery.Schema.of(
      Field.newBuilder("id", StandardSQLTypeName.INT64).setMode(Field.Mode.NULLABLE).build(),
      Field.newBuilder("name", StandardSQLTypeName.STRING).setMode(Field.Mode.NULLABLE).build());

    BatchSinkContext context = Mockito.mock(BatchSinkContext.class);
    SettableArguments arguments = PowerMockito.mock(SettableArguments.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(context.getArguments()).thenReturn(arguments);
    Mockito.when(context.isPreviewEnabled()).thenReturn(new Boolean(Boolean.TRUE));
    Mockito.when(context.getInputSchema()).thenReturn(schema);
    Mockito.when(context.getArguments().get(CmekUtils.CMEK_KEY))
      .thenReturn("projects/project/locations/location/keyRings/key_ring/cryptoKeys/crypto_key");
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.doNothing().when(dataplexServiceClient).close();
    //Setting the stubs for GCPConnectorConfig class.
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("filePath");
    when(dataplexConnectorConfig.getServiceAccountType()).thenReturn("accountType");
    when(dataplexConnectorConfig.tryGetProject()).thenReturn("project");
    when(dataplexConnectorConfig.getProject()).thenReturn("project");
    when(dataplexConnectorConfig.getServiceAccount()).thenReturn("serviceAccount");
    CryptoKeyName cryptoKeyName =
      CryptoKeyName.parse("projects/project/locations/location/keyRings/key_ring/cryptoKeys/crypto_key");
    PowerMockito.mockStatic(BigQueryUtil.class);
    Configuration baseConfiguration = new Configuration();
    Mockito.when(
        BigQueryUtil.getBigQueryConfig(dataplexConnectorConfig.getServiceAccount(), dataplexConnectorConfig.
            getProject(), cryptoKeyName, dataplexConnectorConfig.getServiceAccountType())).
      thenReturn(baseConfiguration);
    Table table = Mockito.mock(Table.class);
    TimePartitioning timePartitioning = Mockito.mock(TimePartitioning.class);
    StandardTableDefinition tableDefinition = Mockito.mock(StandardTableDefinition.class);
    Mockito.when(table.getDefinition()).thenReturn(tableDefinition);
    Mockito.when(tableDefinition.getSchema()).thenReturn(bigQuerySchema);
    Mockito.when(tableDefinition.getTimePartitioning()).thenReturn(timePartitioning);
    Mockito.when(timePartitioning.getField()).thenReturn("field");
    Mockito.when(
      BigQueryUtil.getBigQueryTable(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyBoolean(), Mockito.any())).thenReturn(table);
    Asset asset = mock(Asset.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    // Assigning the values in DataplexBatchSinkConfig class
    DataplexBatchSinkConfig dataplexBatchSinkConfig = Mockito.spy(
      DataplexBatchSinkConfig.builder().setReferenceName("test")
        .setAssetType(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE).setConnection(dataplexConnectorConfig)
        .setUpdateDataplexMetadata(true).setTable("table").setFormat("format").setLake("lake").setZone("zone")
        .setLocation("location").setAsset("asset").setAssetType(Asset.ResourceSpec.Type.BIGQUERY_DATASET.toString())
        .setPartitionByField("field").setClusteringOrder("Order").setDedupeBy("dedupeBy").
        setTableKey("KEY").setRangeStart(1L).setRangeInterval(1L).setRangeEnd(2L).build());

    PowerMockito.mockStatic(GCPUtils.class);
    BigQuery bigQuery = Mockito.mock(BigQuery.class);
    Mockito.when(GCPUtils.getBigQuery("datasetProjectName", googleCredentials)).thenReturn(bigQuery);
    List<BigQueryTableFieldSchema> fields = new ArrayList<>();
    BigQueryTableFieldSchema bigQueryTableFieldSchema = new BigQueryTableFieldSchema();
    fields.add(bigQueryTableFieldSchema);
    PowerMockito.mockStatic(BigQuerySinkUtils.class);
    Mockito.when(BigQuerySinkUtils.getBigQueryTableFields(bigQuery, "table", schema,
        false,
        "datasetProjectName", "datasetName",
        false, mockFailureCollector)).
      thenReturn(fields);
    DatasetId datasetId = DatasetId.of("datasetProjectName", "datasetName");
    Dataset dataset = Mockito.mock(Dataset.class);
    Mockito.when(bigQuery.getDataset(datasetId)).thenReturn(dataset);
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any())).thenReturn(asset);
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.BIGQUERY_DATASET);
    Mockito.when(assetResourceSpec.getName()).thenReturn("projects/datasetProjectName/datasets/datasetName");
    when(dataplexBatchSinkConfig.getCredentials(mockFailureCollector)).thenReturn(googleCredentials);
    DataplexBatchSink dataplexBatchSink = new DataplexBatchSink(dataplexBatchSinkConfig);
    dataplexBatchSink.prepareRun(context);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }


  /**
   * @throws Exception as it will not be able to create the bucket as path is not provided.
   */
  @Test(expected = RuntimeException.class)
  public void testPrepareRunWStorageBucketAssetTypeWithInvalidPath() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("order",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    BatchSinkContext context = Mockito.mock(BatchSinkContext.class);
    SettableArguments arguments = PowerMockito.mock(SettableArguments.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(context.getArguments()).thenReturn(arguments);
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    MetadataServiceClient metadataServiceClient = mock(MetadataServiceClient.class);
    Mockito.when(DataplexUtil.getMetadataServiceClient(googleCredentials)).thenReturn(metadataServiceClient);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.doNothing().when(dataplexServiceClient).close();
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    when(dataplexConnectorConfig.getServiceAccountType()).thenReturn("account");
    when(dataplexConnectorConfig.tryGetProject()).thenReturn("project");
    Asset asset = mock(Asset.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    // Assigning the values in DataplexBatchSinkConfig class
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      Mockito.spy(DataplexBatchSinkConfig.builder()
        .setReferenceName("test").setAssetType(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE).
        setConnection(dataplexConnectorConfig).setUpdateDataplexMetadata(true).setTable("table").
        setFormat("json").setLake("lake").setZone("zone").setLocation("location").setAsset("asset")
        .setAssetType((Asset.ResourceSpec.Type.STORAGE_BUCKET).toString()).build());

    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    when(dataplexBatchSinkConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
      Mockito.any());
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any())).thenReturn(asset);
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.STORAGE_BUCKET);
    Mockito.when(assetResourceSpec.getName()).thenReturn("Project/dataset/table/entity");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField
      ("clusteringOrder"), "order");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField
      ("partitionByField"), "partitionByField");

    dataplexBatchSinkConfig.validateBigQueryDataset(schema, schema, mockFailureCollector, dataplexServiceClient);
    DataplexBatchSink dataplexBatchSink = new DataplexBatchSink(dataplexBatchSinkConfig);
    dataplexBatchSink.prepareRun(context);
  }

  /**
   * @throws Exception as it will not be able to configure the DataplexMetadata
   */
  @Test(expected = RuntimeException.class)
  public void testOnRunFinish() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    BatchSinkContext context = Mockito.mock(BatchSinkContext.class);
    StageMetrics stageMetrics = Mockito.mock(StageMetrics.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(context.getMetrics()).thenReturn(stageMetrics);
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    MetadataServiceClient metadataServiceClient = mock(MetadataServiceClient.class);
    Mockito.when(DataplexUtil.getMetadataServiceClient(googleCredentials)).thenReturn(metadataServiceClient);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.doNothing().when(dataplexServiceClient).close();
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    when(dataplexConnectorConfig.getServiceAccountType()).thenReturn("account");
    when(dataplexConnectorConfig.tryGetProject()).thenReturn("project");
    Asset asset = mock(Asset.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    // Assigning the values in DataplexBatchSinkConfig class
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      Mockito.spy(DataplexBatchSinkConfig.builder()
        .setReferenceName("test").setAssetType(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE).
        setConnection(dataplexConnectorConfig).setUpdateDataplexMetadata(true).setTable("table").
        setFormat("json").setLake("lake").setZone("zone").setLocation("location").setAsset("asset")
        .setAssetType((Asset.ResourceSpec.Type.STORAGE_BUCKET).toString()).build());

    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    when(dataplexBatchSinkConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
      Mockito.any());
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    Schema.PartitionField.Builder partitionFieldBuilder = Schema.PartitionField.newBuilder();
    partitionFieldBuilder.setName("name");
    partitionFieldBuilder.setType(Schema.Type.STRING);
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName("name");
    fieldBuilder.setType(Schema.Type.RECORD);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).
      addPartitionFields(partitionFieldBuilder.build()).build();
    PowerMockito.mockStatic(DataplexUtil.class);
    PowerMockito.when(DataplexUtil.getDataplexSchema(schema)).thenReturn(dataplexSchema);
    PowerMockito.when(DataplexUtil.getStorageFormatForEntity(dataplexBatchSinkConfig.getFormatStr())).
      thenReturn("entity");
    when(dataplexBatchSinkConfig.getCredentials(mockFailureCollector)).thenReturn(googleCredentials);
    DataplexBatchSink dataplexBatchSink = new DataplexBatchSink(dataplexBatchSinkConfig);
    FieldSetter.setField(dataplexBatchSink, DataplexBatchSink.class.getDeclaredField("asset"), asset);
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.STORAGE_BUCKET);
    Mockito.when(assetResourceSpec.getName()).thenReturn("name");
    dataplexBatchSink.onRunFinish(true, context);
  }

  /**
   * @throws Exception as it will not be able to configure the DataplexMetadata
   */
  @Test(expected = RuntimeException.class)
  public void testOnRunFinishWithEntityBean() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));

    BatchSinkContext context = Mockito.mock(BatchSinkContext.class);
    StageMetrics stageMetrics = Mockito.mock(StageMetrics.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(context.getMetrics()).thenReturn(stageMetrics);
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.doNothing().when(dataplexServiceClient).close();
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    when(dataplexConnectorConfig.getServiceAccountType()).thenReturn("account");
    when(dataplexConnectorConfig.tryGetProject()).thenReturn("project");
    Asset asset = mock(Asset.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    // Assigning the values in DataplexBatchSinkConfig class
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      Mockito.spy(DataplexBatchSinkConfig.builder()
        .setReferenceName("test").setAssetType(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE).
        setConnection(dataplexConnectorConfig).setUpdateDataplexMetadata(true).setTable("table").
        setFormat("json").setLake("lake").setZone("zone").setLocation("location").setAsset("asset").
        setAssetType((Asset.ResourceSpec.Type.STORAGE_BUCKET).toString()).build());

    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    when(dataplexBatchSinkConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
      Mockito.any());
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    Schema.PartitionField.Builder partitionFieldBuilder = Schema.PartitionField.newBuilder();
    partitionFieldBuilder.setName("name");
    partitionFieldBuilder.setType(Schema.Type.STRING);
    Schema.SchemaField.Builder fieldBuilder = Schema.SchemaField.newBuilder();
    fieldBuilder.setName("name");
    fieldBuilder.setType(Schema.Type.RECORD);
    Schema.SchemaField schemaField = fieldBuilder.build();
    com.google.cloud.dataplex.v1.Schema dataplexSchema = Schema.newBuilder().addFields(schemaField).
      addPartitionFields(partitionFieldBuilder.build()).build();
    PowerMockito.mockStatic(DataplexUtil.class);
    PowerMockito.when(DataplexUtil.getDataplexSchema(schema)).thenReturn(dataplexSchema);
    PowerMockito.when(DataplexUtil.getStorageFormatForEntity(dataplexBatchSinkConfig.getFormatStr())).
      thenReturn("entity");
    when(dataplexBatchSinkConfig.getCredentials(mockFailureCollector)).thenReturn(googleCredentials);
    DataplexBatchSink dataplexBatchSink = new DataplexBatchSink(dataplexBatchSinkConfig);
    Entity entity = Mockito.mock(Entity.class);
    //Setting the reflection fields.
    FieldSetter.setField(dataplexBatchSink, DataplexBatchSink.class.getDeclaredField("asset"), asset);
    FieldSetter.setField(dataplexBatchSink, DataplexBatchSink.class.getDeclaredField("entityBean"), entity);

    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.STORAGE_BUCKET);
    Mockito.when(assetResourceSpec.getName()).thenReturn("name");
    Mockito.when(entity.getName()).thenReturn("name");
    Mockito.when(entity.getEtag()).thenReturn("name");
    dataplexBatchSink.onRunFinish(true, context);
  }

}
