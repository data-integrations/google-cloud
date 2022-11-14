/*
 * Copyright © 2022 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.dataplex.source;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.dataplex.v1.Asset;
import com.google.cloud.dataplex.v1.AssetName;
import com.google.cloud.dataplex.v1.DataplexServiceClient;
import com.google.cloud.dataplex.v1.Entity;
import com.google.cloud.dataplex.v1.EntityName;
import com.google.cloud.dataplex.v1.GetEntityRequest;
import com.google.cloud.dataplex.v1.Lake;
import com.google.cloud.dataplex.v1.LakeName;
import com.google.cloud.dataplex.v1.MetadataServiceClient;
import com.google.cloud.dataplex.v1.OperationMetadata;
import com.google.cloud.dataplex.v1.StorageSystem;
import com.google.cloud.dataplex.v1.Task;
import com.google.cloud.dataplex.v1.Zone;
import com.google.cloud.dataplex.v1.ZoneName;
import com.google.cloud.storage.Storage;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.action.SettableArguments;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.batch.ConfigurationUtils;
import io.cdap.plugin.common.batch.JobUtils;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceUtils;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexConstants;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexUtil;
import io.cdap.plugin.gcp.dataplex.source.config.DataplexBatchSourceConfig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for DataplexBatchSource
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DataplexBatchSource.class, DataplexBatchSourceConfig.class, BigQueryUtil.class, DataplexUtil.class,
  GCPUtils.class, JobUtils.class, ServiceAccountCredentials.class, ConfigurationUtils.class,
  BigQuerySourceUtils.class, FileSystem.class})
public class DataplexBatchSourceTest {
  @Mock
  StatusCode statusCode;
  @Mock
  private GCPConnectorConfig connection;

  private DataplexBatchSourceConfig.Builder getBuilder() {
    return DataplexBatchSourceConfig.builder()
      .setReferenceName("test");
  }

  @Test
  public void tryGetProjectTest() {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig config = spy(builder.build());
    when(config.tryGetProject()).thenReturn(null);
    assertNull(config.tryGetProject());
    verify(config, times(1)).tryGetProject();
  }

  @Test
  public void validateEntityConfigWhenLakeIsNull() throws IOException {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GoogleCredentials credentials = PowerMockito.mock(GoogleCredentials.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig =
      PowerMockito.spy(builder.setLake(null).setLocation("test").build());
    doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    Entity entity = dataplexBatchSourceConfig.getAndValidateEntityConfiguration(mockFailureCollector, credentials);
    assertEquals(null, entity);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccount() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GoogleCredentials credentials = PowerMockito.mock(GoogleCredentials.class);
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig config = spy(builder.setConnection(connection).build());
    doReturn(null).when(config).getCredentials(mockFailureCollector);
    try {
      config.validateAndGetServiceAccountCredentials(mockFailureCollector);
    } catch (Exception e) {
    }
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountIsServiceAccountJsonTrue() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig config = spy(builder.setConnection(connection).build());
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    doReturn(googleCredentials).when(config).getCredentials(mockFailureCollector);
    config.validateAndGetServiceAccountCredentials(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountWhenIsServiceAccountJsonTrueAndFilePathIsNotNull() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig config = spy(builder.setConnection(connection).build());
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    doReturn(googleCredentials).when(config).getCredentials(mockFailureCollector);
    config.validateAndGetServiceAccountCredentials(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testConfigurePipeline() throws IOException, NoSuchFieldException {
    io.cdap.cdap.api.data.schema.Schema schema =
      io.cdap.cdap.api.data.schema.Schema.recordOf("record", io.cdap.cdap.api.data.schema.Schema.
        Field.of("ts", io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));
    Entity entity = Mockito.mock(Entity.class);
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
    DataplexBatchSourceConfig dataplexBatchSourceConfig =
      Mockito.spy(DataplexBatchSourceConfig.builder()
        .setReferenceName("test").
        setConnection(dataplexConnectorConfig).build());
    when(dataplexBatchSourceConfig.getCredentials(mockFailureCollector)).thenReturn(googleCredentials);
    when(dataplexBatchSourceConfig.getConnection().canConnect()).thenReturn(true);
    DataplexBatchSource dataplexBatchSource = new DataplexBatchSource(dataplexBatchSourceConfig);
    FieldSetter.setField(dataplexBatchSource, DataplexBatchSource.class.getDeclaredField("entity"), entity);
    FieldSetter.setField(dataplexBatchSource, DataplexBatchSource.class.getDeclaredField("outputSchema"), schema);
    Mockito.when(entity.getSystem()).thenReturn(StorageSystem.BIGQUERY);
    dataplexBatchSource.configurePipeline(pipelineConfigurer);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  /**
   * @throws Exception as unable to login.
   * It creates different path everytime, so this exception can't be handled through mocking.
   */
  @Test(expected = IOException.class)
  public void testPrepareRunWithStorageBucketSystem() throws Exception {
    BatchRuntimeContext runtimeContext = Mockito.mock(BatchRuntimeContext.class);
    io.cdap.cdap.api.data.schema.Schema schema =
      io.cdap.cdap.api.data.schema.Schema.recordOf("record", io.cdap.cdap.api.data.schema.Schema.Field.of("order", io.
        cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));
    BatchSourceContext context = Mockito.mock(BatchSourceContext.class);
    SettableArguments arguments = PowerMockito.mock(SettableArguments.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(runtimeContext.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(context.getArguments()).thenReturn(arguments);
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    MetadataServiceClient metadataServiceClient = mock(MetadataServiceClient.class);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.when(DataplexUtil.getMetadataServiceClient(googleCredentials)).thenReturn(metadataServiceClient);
    Mockito.doNothing().when(dataplexServiceClient).close();
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.isServiceAccountFilePath()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    when(dataplexConnectorConfig.getServiceAccountType()).thenReturn("account");
    when(dataplexConnectorConfig.tryGetProject()).thenReturn("project");
    when(dataplexConnectorConfig.getServiceAccount()).thenReturn("serviceAccount");
    Asset asset = mock(Asset.class);
    Entity entity = Mockito.mock(Entity.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    // Setting the values in config class.
    DataplexBatchSourceConfig dataplexBatchSourceConfig =
      Mockito.spy(DataplexBatchSourceConfig.builder()
        .setReferenceName("test").setReferenceName("reference").
        setConnection(dataplexConnectorConfig).setLake("lake").setZone("zone").setLocation("location")
        .setEntity("entity").setPartitionTo("s").setpartitionFrom("f").setFilter("filter").build());

    // Mocked the values and assigned the stub values to the methods needed.
    Mockito.when(DataplexUtil.getMetadataServiceClient(dataplexBatchSourceConfig.getCredentials(context.
      getFailureCollector()))).thenReturn(metadataServiceClient);
    Mockito.when(metadataServiceClient.getEntity(GetEntityRequest.newBuilder().setName(EntityName.of("project",
        "location", "lake", "zone", "entity").toString()).setView
      (GetEntityRequest.EntityView.FULL).build())).thenReturn(entity);
    PowerMockito.mockStatic(BigQueryUtil.class);
    Table table = Mockito.mock(Table.class);
    doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    Mockito.when(entity.getSystem()).thenReturn(StorageSystem.CLOUD_STORAGE);
    Mockito.when(entity.getDataPath()).thenReturn("project/dataset/table");
    Mockito.when(BigQueryUtil.getBigQueryTable(Mockito.anyString(), Mockito.anyString(),
      Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),
      Mockito.any())).thenReturn(table);
    PowerMockito.mockStatic(BigQueryUtil.class);
    Configuration configuration = Mockito.mock(Configuration.class);
    BigQuery bigQuery = Mockito.mock(BigQuery.class);
    PowerMockito.mockStatic(GCPUtils.class);
    Storage storage = Mockito.mock(Storage.class);
    Mockito.when(GCPUtils.getBigQuery("dataset", googleCredentials)).thenReturn(bigQuery);
    PowerMockito.when(BigQueryUtil.getBigQueryConfig(null, null, null,
      "account")).thenReturn(configuration);
    Dataset dataset = Mockito.mock(Dataset.class);
    Mockito.when(bigQuery.getDataset(DatasetId.of("dataset", "project"))).thenReturn(dataset);
    when(dataplexBatchSourceConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    doReturn("abc@gmail.com").when(dataplexBatchSourceConfig).getServiceAccountEmail();
    Job job = Mockito.mock(Job.class);
    PowerMockito.mockStatic(JobUtils.class);
    Mockito.when(JobUtils.createInstance()).thenReturn(job);
    when(job.getConfiguration()).thenReturn(configuration);
    doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any())).thenReturn(asset);
    OperationFuture<Task, OperationMetadata> future = Mockito.mock(OperationFuture.class);
    when(dataplexServiceClient.createTaskAsync((LakeName) any(), any(), Mockito.anyString())).thenReturn(future);
    when(future.get()).thenReturn(Task.newBuilder().setDescription("description").build());
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.STORAGE_BUCKET);
    Mockito.when(assetResourceSpec.getName()).thenReturn("Project/dataset/table/entity");
    InputStream inputStream = Mockito.mock(InputStream.class);
    PowerMockito.mockStatic(GCPUtils.class);
    Mockito.when(GCPUtils.openServiceAccount("serviceAccount", true)).thenReturn(inputStream);
    Mockito.when(GCPUtils.getStorage("project", googleCredentials)).thenReturn(storage);
    PowerMockito.mockStatic(ServiceAccountCredentials.class);
    ServiceAccountCredentials credentials = Mockito.mock(ServiceAccountCredentials.class);
    Mockito.when(ServiceAccountCredentials.fromStream(inputStream)).thenReturn(credentials);
    DataplexBatchSource dataplexBatchSource = new DataplexBatchSource(dataplexBatchSourceConfig);
    //Setting the reflection field.
    FieldSetter.setField(dataplexBatchSource, DataplexBatchSource.class.getDeclaredField("entity"), entity);

    ValidatingInputFormat validatingInputFormat = Mockito.mock(ValidatingInputFormat.class);
    Mockito.when(context.newPluginInstance("avro")).thenReturn(validatingInputFormat);
    PowerMockito.mockStatic(BigQuerySourceUtils.class);
    dataplexBatchSource.onRunFinish(true, context);
    dataplexBatchSource.initialize(runtimeContext);
    dataplexBatchSource.prepareRun(context);
  }

  @Test
  public void testPrepareRunWithBigquerySystem() throws Exception {
    BatchRuntimeContext runtimeContext = Mockito.mock(BatchRuntimeContext.class);
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("order",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));
    BatchSourceContext context = Mockito.mock(BatchSourceContext.class);
    SettableArguments arguments = PowerMockito.mock(SettableArguments.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Mockito.when(context.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(runtimeContext.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(context.getArguments()).thenReturn(arguments);
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    MetadataServiceClient metadataServiceClient = mock(MetadataServiceClient.class);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.when(DataplexUtil.getMetadataServiceClient(googleCredentials)).thenReturn(metadataServiceClient);
    Mockito.when(DataplexUtil.getTableSchema(null, mockFailureCollector)).thenReturn(schema);
    Mockito.doNothing().when(dataplexServiceClient).close();
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    when(dataplexConnectorConfig.getServiceAccountType()).thenReturn("account");
    when(dataplexConnectorConfig.tryGetProject()).thenReturn("project");
    Asset asset = mock(Asset.class);
    Entity entity = Mockito.mock(Entity.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    // Assigning the values in DataplexBatchSourceConfig class
    DataplexBatchSourceConfig dataplexBatchSourceConfig =
      Mockito.spy(DataplexBatchSourceConfig.builder()
        .setReferenceName("test").setReferenceName("reference").
        setConnection(dataplexConnectorConfig).setLake("lake").setZone("zone").setLocation("location")
        .setEntity("entity").setPartitionTo("s").setpartitionFrom("f").setFilter("filter").build());

    Mockito.when(DataplexUtil.getMetadataServiceClient(dataplexBatchSourceConfig.getCredentials(context.
      getFailureCollector()))).thenReturn(metadataServiceClient);
    Mockito.when(metadataServiceClient.getEntity(GetEntityRequest.newBuilder().setName(EntityName.of("project",
        "location", "lake", "zone", "entity").toString()).setView
      (GetEntityRequest.EntityView.FULL).build())).thenReturn(entity);
    PowerMockito.mockStatic(BigQueryUtil.class);
    Table table = Mockito.mock(Table.class);
    doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    // Value of system is BIGQUERY.
    Mockito.when(entity.getSystem()).thenReturn(StorageSystem.BIGQUERY);
    Mockito.when(entity.getDataPath()).thenReturn("project/dataset/table");
    Mockito.when(BigQueryUtil.getBigQueryTable(Mockito.anyString(), Mockito.anyString(),
      Mockito.anyString(), Mockito.anyString(), Mockito.anyBoolean(),
      Mockito.any())).thenReturn(table);
    PowerMockito.mockStatic(BigQueryUtil.class);
    Configuration configuration = Mockito.mock(Configuration.class);
    String tableName = "cdap.bq.source.temporary.table.name";
    Mockito.when(configuration.get(DataplexConstants.DATAPLEX_ENTITY_TYPE)).thenReturn(StorageSystem.CLOUD_STORAGE.
      toString());
    Mockito.when(configuration.get(tableName)).thenReturn("table");
    BigQuery bigQuery = Mockito.mock(BigQuery.class);
    PowerMockito.mockStatic(GCPUtils.class);
    Map<String, String> map = new HashMap<>();
    map.put("price", "100");
    Mockito.when(GCPUtils.getBigQuery("dataset", googleCredentials)).thenReturn(bigQuery);
    PowerMockito.mockStatic(ConfigurationUtils.class);
    Mockito.when(ConfigurationUtils.getNonDefaultConfigurations(configuration)).thenReturn(map);
    PowerMockito.when(BigQueryUtil.getBigQueryConfig(null, null, null,
      "account")).thenReturn(configuration);
    Dataset dataset = Mockito.mock(Dataset.class);
    Mockito.when(bigQuery.getDataset(DatasetId.of("dataset", "project"))).thenReturn(dataset);
    when(dataplexBatchSourceConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any())).thenReturn(asset);
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.STORAGE_BUCKET);
    Mockito.when(assetResourceSpec.getName()).thenReturn("Project/dataset/table/entity");
    DataplexBatchSource dataplexBatchSource = new DataplexBatchSource(dataplexBatchSourceConfig);
    Whitebox.setInternalState(DataplexBatchSource.class, "outputSchema", schema);
    //Setting the reflection fields.
    FieldSetter.setField(dataplexBatchSource, DataplexBatchSource.class.getDeclaredField("entity"), entity);
    FieldSetter.setField(dataplexBatchSource, DataplexBatchSource.class.getDeclaredField("configuration"),
      configuration);
    
    dataplexBatchSource.initialize(runtimeContext);
    dataplexBatchSource.prepareRun(context);
    Mockito.when(GCPUtils.getBigQuery(null, googleCredentials)).thenReturn(bigQuery);
    dataplexBatchSource.onRunFinish(true, context);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }
}
