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

package io.cdap.plugin.gcp.dataplex.source.config;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.Table;
import com.google.cloud.dataplex.v1.DataplexServiceClient;
import com.google.cloud.dataplex.v1.Entity;
import com.google.cloud.dataplex.v1.EntityName;
import com.google.cloud.dataplex.v1.GetEntityRequest;
import com.google.cloud.dataplex.v1.Lake;
import com.google.cloud.dataplex.v1.LakeName;
import com.google.cloud.dataplex.v1.MetadataServiceClient;
import com.google.cloud.dataplex.v1.Zone;
import com.google.cloud.dataplex.v1.ZoneName;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.cdap.etl.api.validation.ValidatingInputFormat;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexUtil;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for DataplexBatchSourceConfig
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DataplexUtil.class, BigQueryUtil.class, GCPUtils.class})
public class DataplexBatchSourceConfigTest {

  @InjectMocks
  DataplexBatchSourceConfig dataplexBatchSourceConfig;

  @Mock
  StatusCode statusCode;

  @Mock
  private GCPConnectorConfig connection;

  private DataplexBatchSourceConfig.Builder getBuilder() {
    return DataplexBatchSourceConfig.builder()
      .setReferenceName("test");
  }


  @Test
  public void testGetProject() {
    Assert.assertNull(dataplexBatchSourceConfig.getProject());
  }

  @Test
  public void testTryGetProject() {
    assertNull((dataplexBatchSourceConfig).tryGetProject());
  }

  @Test
  public void testGetServiceAccountType() {
    assertNull((dataplexBatchSourceConfig).getServiceAccountType());
  }

  @Test
  public void testGetPartitionFrom() {
    assertNull((dataplexBatchSourceConfig).getPartitionFrom());
  }

  @Test
  public void testGetPartitionTo() {
    assertNull((dataplexBatchSourceConfig).getPartitionTo());
  }

  @Test
  public void testGetFilter() {
    assertNull((dataplexBatchSourceConfig).getFilter());
  }

  @Test
  public void testIsServiceAccountFilePath() {
    Assert.assertFalse((dataplexBatchSourceConfig).isServiceAccountFilePath());
  }

  @Test
  public void testGetSchema() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    assertNull(dataplexBatchSourceConfig.getSchema(mockFailureCollector));
  }

  @Test
  public void testValidateTable() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    dataplexBatchSourceConfig = DataplexBatchSourceConfig.builder()
      .setReferenceName("test").build();
    try {
      dataplexBatchSourceConfig.validateBigQueryDataset(mockFailureCollector,
        "project", "dataset", "table.wrong");
    } catch (Exception e) {
    }

    assertTrue("Expected at least one error with incorrect table name",
      mockFailureCollector.getValidationFailures().size() > 0);

    mockFailureCollector.getValidationFailures().stream()
      .filter(error -> error.getFullMessage().toLowerCase().contains("table name"))
      .findFirst().orElseThrow(
        () -> new AssertionError("Validation Errors didn't contain an error referring to table name"));
  }

  @Test
  public void testGetSchemaWithException() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig dataplexBatchSourceConfig = builder.build();
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    String schema = "Schema.recordOf(\"record\",\n" +
      "                                         Schema.Field.of(\"storeid\", Schema.of(Schema.Type.STRING)),\n" +
      "                                         Schema.Field.of(\"price\", " +
      "                                         Schema.nullableOf(Schema.decimalOf(4, 2))),\n" +
      "                                         Schema.Field.of(\"timestamp\",\n" +
      "                                                         Schema.nullableOf(Schema.of\n" +
      "                                                           (Schema.LogicalType.TIMESTAMP_MICROS))),\n" +
      "                                         Schema.Field.of(\"date\", Schema.of(Schema.LogicalType.DATE)))";
    dataplexBatchSourceConfig = builder.setSchema(schema).build();
    try {
      dataplexBatchSourceConfig.getSchema(mockFailureCollector);
      Assert.fail("Exception would not have thrown with valid schema.");
    } catch (ValidationException e) {
      Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
    }

  }

  @Test
  public void testGetFilterNotNull() {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig dataplexBatchSourceConfig = builder.build();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    dataplexBatchSourceConfig = builder.setFilter("Filter").build();
    dataplexBatchSourceConfig.getFilter().equals(null);
    Assert.assertNotNull(dataplexBatchSourceConfig.getFilter().isEmpty());
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateEntityConfigWhenLocationIsNull() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig = PowerMockito.spy(builder.build());
    Mockito.doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    dataplexBatchSourceConfig.getAndValidateEntityConfiguration(mockFailureCollector, googleCredentials);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateEntityConfigWhenLocationIsNotNull() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig = PowerMockito.spy(builder.setLocation("Location").build());
    Mockito.doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    dataplexBatchSourceConfig.getAndValidateEntityConfiguration(mockFailureCollector, googleCredentials);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }


  @Test
  public void validateEntityConfigWhenLakeIsNull() throws IOException {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GoogleCredentials credentials = Mockito.mock(GoogleCredentials.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig =
      PowerMockito.spy(builder.setLake(null).setLocation("test").build());
    Mockito.doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    Entity entity = dataplexBatchSourceConfig.getAndValidateEntityConfiguration(mockFailureCollector, credentials);
    Assert.assertEquals(null, entity);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateEntityConfigWhenLakeIsNotNull() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = Mockito.mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig = PowerMockito.spy(builder.setLake("lake").
      setLocation("location").build());
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    Mockito.doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    Mockito.when(dataplexServiceClient.getLake((LakeName) Mockito.any())).thenReturn(Lake.newBuilder().build());
    dataplexBatchSourceConfig.getAndValidateEntityConfiguration(mockFailureCollector, googleCredentials);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateEntityConfigWhenLakeIsNotNullThrows() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = Mockito.mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    ApiException apiException = new ApiException(new RuntimeException(), statusCode, false);
    Mockito.when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
    DataplexBatchSourceConfig dataplexBatchSourceConfig =
      PowerMockito.spy(builder.setLake("lake").setLocation("location").build());
    Mockito.doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    Mockito.doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.when(dataplexServiceClient.getLake((LakeName) Mockito.any())).thenThrow(apiException);
    dataplexBatchSourceConfig.getAndValidateEntityConfiguration(mockFailureCollector, googleCredentials);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateEntityConfigWhenZoneIsNotNull() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = Mockito.mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    Lake lake = Mockito.mock(Lake.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig = PowerMockito.spy
      (builder.setZone("zone").setLake("lake").setLocation("location").build());
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    Mockito.doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    Mockito.when(dataplexServiceClient.getLake((LakeName) Mockito.any())).thenReturn(lake);
    Mockito.when(dataplexServiceClient.getZone((ZoneName) Mockito.any())).thenReturn(Zone.newBuilder().build());
    dataplexBatchSourceConfig.getAndValidateEntityConfiguration(mockFailureCollector, googleCredentials);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateEntityConfigWhenZoneIsNotNullAndThrowsException() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = Mockito.mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    ApiException apiException = new ApiException(new RuntimeException(), statusCode, false);
    PowerMockito.mockStatic(DataplexUtil.class);
    Lake lake = Mockito.mock(Lake.class);
    Mockito.when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
    DataplexBatchSourceConfig dataplexBatchSourceConfig =
      PowerMockito.spy(builder.setLake("example-lake").setLocation("test").setZone("example-zone").build());
    Mockito.doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    Mockito.when(dataplexServiceClient.getLake((LakeName) Mockito.any())).thenReturn(lake);
    Mockito.when(dataplexServiceClient.getZone((ZoneName) Mockito.any())).thenThrow(apiException);
    dataplexBatchSourceConfig.getAndValidateEntityConfiguration(mockFailureCollector, googleCredentials);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateEntityConfigWhenConfigEntityIsNotNull() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    MetadataServiceClient metadataServiceClient = Mockito.mock(MetadataServiceClient.class);
    DataplexServiceClient dataplexServiceClient = Mockito.mock(DataplexServiceClient.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    Lake lake = Mockito.mock(Lake.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig = PowerMockito.spy
      (builder.setZone(null).setLake(null).setLocation("location").build());
    Mockito.when(DataplexUtil.getMetadataServiceClient(googleCredentials)).thenReturn(metadataServiceClient);
    Mockito.doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    Mockito.doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    Mockito.when(dataplexServiceClient.getLake((LakeName) Mockito.any())).thenReturn(lake);
    Mockito.when(dataplexServiceClient.getZone((ZoneName) Mockito.any())).thenReturn(Zone.newBuilder().build());
    Mockito.when(metadataServiceClient.getEntity((GetEntityRequest) Mockito.any())).thenReturn(Entity.newBuilder().
      build());
    dataplexBatchSourceConfig.getAndValidateEntityConfiguration(mockFailureCollector, googleCredentials);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());

  }

  @Test
  public void validateEntityConfigWhenEntityIsNotNull() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    Lake lake = Mockito.mock(Lake.class);
    ApiException apiException = new ApiException(new RuntimeException(), statusCode, false);
    Mockito.when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
    DataplexBatchSourceConfig dataplexBatchSourceConfig =
      PowerMockito.spy(builder.setZone("zone").setLake("lake").setLocation("test").setReferenceName("referenceName").
        build());
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
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
    Mockito.doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    Mockito.doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    Mockito.when(dataplexServiceClient.getLake((LakeName) Mockito.any())).thenReturn(lake);
    Mockito.when(dataplexServiceClient.getZone((ZoneName) Mockito.any())).thenReturn(Zone.newBuilder().build());
    Mockito.when(metadataServiceClient.getEntity((GetEntityRequest.newBuilder().
        setName(EntityName.format("project", "location", "lake", "zone", "entity")).
        setView(GetEntityRequest.EntityView.FULL).build()))).
      thenThrow(apiException);
    dataplexBatchSourceConfig.getAndValidateEntityConfiguration(mockFailureCollector, googleCredentials);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountWithCredentialsNull() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig config = Mockito.spy(builder.setConnection(connection).build());
    Mockito.doReturn(null).when(config).getCredentials(mockFailureCollector);
    config.validateAndGetServiceAccountCredentials(mockFailureCollector);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountIsServiceAccountJsonTrue() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig config = Mockito.spy(builder.setConnection(connection).build());
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    Mockito.doReturn(googleCredentials).when(config).getCredentials(mockFailureCollector);
    config.validateAndGetServiceAccountCredentials(mockFailureCollector);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountWhenIsServiceAccountJsonTrueAndFilePathIsNotNull() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig config = Mockito.spy(builder.setConnection(connection).build());
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    Mockito.doReturn(googleCredentials).when(config).getCredentials(mockFailureCollector);
    config.validateAndGetServiceAccountCredentials(mockFailureCollector);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBigQuerySourceTable() throws Exception {
    DataplexBatchSourceConfig dataplexBatchSourceConfig = PowerMockito.spy(getBuilder().build());
    GoogleCredentials credentials = Mockito.mock(GoogleCredentials.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector("stage name");
    Table table = PowerMockito.mock(Table.class);
    PowerMockito.mockStatic(BigQueryUtil.class);
    Mockito.when(BigQueryUtil.getBigQueryTable("project", "datasets", "tablename",
      "service", true,
      mockFailureCollector)).thenReturn(table);
    Mockito.doReturn(credentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    Mockito.doReturn(credentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    dataplexBatchSourceConfig.validateBigQueryDataset(mockFailureCollector, "project",
      "dataset", "table");
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBigQueryPartitionFromIsNull() {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig dataplexBatchSourceConfig = builder.build();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    dataplexBatchSourceConfig = builder.setpartitionFrom(null).build();
    dataplexBatchSourceConfig.validateBigQueryDataset(mockFailureCollector, "project",
      "data-set", "table");
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBigQueryPartitionToIsNull() {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig dataplexBatchSourceConfig = builder.build();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    dataplexBatchSourceConfig = builder.setPartitionTo(null).build();
    dataplexBatchSourceConfig.validateBigQueryDataset(mockFailureCollector, "project",
      "data-set", "table");
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testSetUpValidatingInputFormat() {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = Mockito.mock(MockFailureCollector.class);
    PipelineConfigurer pipelineConfigurer = Mockito.mock(PipelineConfigurer.class);
    StageConfigurer stageConfigurer = Mockito.mock(StageConfigurer.class);
    Mockito.when(pipelineConfigurer.getStageConfigurer()).thenReturn(stageConfigurer);
    Entity entity = Mockito.mock(Entity.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig = PowerMockito.spy(builder.setSchema(null).build());
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.
        schema.Schema.Field.
        of("id", io.cdap.
          cdap.api.data.schema.
          Schema.of
            (io.cdap.cdap.api.data.
              schema.Schema.Type.
              LONG)));
    Mockito.when(DataplexUtil.getTableSchema(entity.getSchema(), mockFailureCollector)).thenReturn(schema);
    dataplexBatchSourceConfig.setupValidatingInputFormat(pipelineConfigurer, mockFailureCollector, entity);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testFileSystemProperties() {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig dataplexBatchSourceConfig = PowerMockito.spy(builder.build());
    Map<String, String> properties = new HashMap<>();
    properties.put("key", "value");
    PowerMockito.mockStatic(GCPUtils.class);
    Mockito.when(GCPUtils.getFileSystemProperties(Mockito.any(), Mockito.anyString(), Mockito.anyMap())).
      thenReturn(properties);
    String path = "project/dataset";
    Assert.assertEquals(1, dataplexBatchSourceConfig.getFileSystemProperties(path).size());
  }

  @Test
  public void testGetValidatingInputFormat() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig dataplexBatchSourceConfig = PowerMockito.spy(builder.build());
    BatchSourceContext batchSourceContext = Mockito.mock(BatchSourceContext.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    ValidatingInputFormat validatingInputFormat = Mockito.mock(ValidatingInputFormat.class);
    String fileFormat = "format";
    Mockito.when(batchSourceContext.getFailureCollector()).thenReturn(mockFailureCollector);
    Mockito.when(batchSourceContext.newPluginInstance(fileFormat)).thenReturn(validatingInputFormat);
    try {
      dataplexBatchSourceConfig.getValidatingInputFormat(batchSourceContext);
      Assert.fail("Exception will not be thrown for correct format");
    } catch (ValidationException e) {
      Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
    }
  }

  @Test
  public void testCheckMetaStoreForGCSLakeIsNull() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GoogleCredentials credentials = Mockito.mock(GoogleCredentials.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig =
      PowerMockito.spy(builder.setLake(null).build());
    Mockito.doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    dataplexBatchSourceConfig.checkMetastoreForGCSEntity(mockFailureCollector, credentials);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testCheckMetaStoreForGCSLakeIsNotNull() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = Mockito.mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    Lake lake = Mockito.mock(Lake.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig = PowerMockito.spy(builder.
      setLake("lake").setLocation("location").
      build());
    Mockito.when(lake.getMetastore()).thenReturn(Lake.Metastore.newBuilder().build());
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    Mockito.doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    Mockito.when(dataplexServiceClient.getLake((LakeName) Mockito.any())).thenReturn(lake);
    dataplexBatchSourceConfig.checkMetastoreForGCSEntity(mockFailureCollector, googleCredentials);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testCheckMetaStoreForGCSLakeNull() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = Mockito.mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    Lake lake = Mockito.mock(Lake.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig = PowerMockito.spy(builder.
      setLake("lake").
      setLocation("location").build());
    Mockito.when(lake.getMetastore()).thenReturn(null);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    Mockito.doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    Mockito.when(dataplexServiceClient.getLake((LakeName) Mockito.any())).thenReturn(lake);
    try {
      dataplexBatchSourceConfig.checkMetastoreForGCSEntity(mockFailureCollector, googleCredentials);
      Assert.fail("Exception will not be thrown for not null value");
    } catch (Exception e) {
      Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
    }
  }

  @Test
  public void testCheckMetaStoreForLakeBean() throws Exception {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = Mockito.mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig;
    dataplexBatchSourceConfig = PowerMockito.spy
      (builder.setLake(null).setLocation("Location").build());
    Mockito.doReturn(googleCredentials).when(dataplexBatchSourceConfig).getCredentials(mockFailureCollector);
    Mockito.when(DataplexUtil.getDataplexServiceClient(googleCredentials)).thenReturn(dataplexServiceClient);
    Mockito.doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    dataplexBatchSourceConfig.checkMetastoreForGCSEntity(mockFailureCollector, googleCredentials);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

}
