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

package io.cdap.plugin.gcp.dataplex.sink.config;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.RangePartitioning;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.dataplex.v1.Asset;
import com.google.cloud.dataplex.v1.AssetName;
import com.google.cloud.dataplex.v1.DataplexServiceClient;
import com.google.cloud.dataplex.v1.Lake;
import com.google.cloud.dataplex.v1.LakeName;
import com.google.cloud.dataplex.v1.Zone;
import com.google.cloud.dataplex.v1.ZoneName;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.validation.FormatContext;
import io.cdap.cdap.etl.api.validation.ValidatingOutputFormat;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexConstants;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexUtil;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DataplexBatchSinkConfig.class, Schema.class, DataplexServiceClient.class, DataplexUtil.class,
  BigQueryUtil.class})
public class DataplexBatchSinkConfigTest {

  @Mock
  StatusCode statusCode;

  private DataplexBatchSinkConfig.Builder getBuilder() {
    return DataplexBatchSinkConfig.builder()
      .setReferenceName("test");
  }

  @Test
  public void validateBigQueryDatasetTest() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    DataplexBatchSinkConfig dataplexBatchSinkConfig = builder.build();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    dataplexBatchSinkConfig = builder.setTable("test").build();
    dataplexBatchSinkConfig.validateBigQueryDataset(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateBigQueryDatasetWhenTableIsNull() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    DataplexBatchSinkConfig dataplexBatchSinkConfig = builder.build();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    dataplexBatchSinkConfig = builder.setTable(null).build();
    try {
      dataplexBatchSinkConfig.validateBigQueryDataset(mockFailureCollector);
    } catch (Exception e) {
    }
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateBigQueryDatasetWhenTruncateTableIsNotNull() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    DataplexBatchSinkConfig dataplexBatchSinkConfig = builder.build();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    dataplexBatchSinkConfig = builder.setTable("test")
      .setTruncateTable(true).build();
    dataplexBatchSinkConfig.validateBigQueryDataset(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateBigQueryDatasetWhenTruncateTableAndOperationIsNotNull() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    DataplexBatchSinkConfig dataplexBatchSinkConfig;
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    dataplexBatchSinkConfig = builder.setTable("test")
      .setTruncateTable(true)
      .setOperation("UPDATE").build();
    dataplexBatchSinkConfig.validateBigQueryDataset(mockFailureCollector);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAndGetServiceAccountCredentialsWhenIsServiceAccountJsonTrue() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    DataplexBatchSinkConfig dataplexBatchSinkConfig;
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    dataplexBatchSinkConfig = PowerMockito.spy(builder.setConnection(dataplexConnectorConfig).build());
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));

    try {
      dataplexBatchSinkConfig.validateAndGetServiceAccountCredentials(mockFailureCollector);
    } catch (Exception e) {
      e.getMessage();
    }
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAndGetServiceAccountCredentialsWhenIsServiceAccountJsonTrueAndFilePathIsNotNull() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    DataplexBatchSinkConfig dataplexBatchSinkConfig;
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    dataplexBatchSinkConfig = builder.setConnection(dataplexConnectorConfig).build();
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    try {
      dataplexBatchSinkConfig.validateAndGetServiceAccountCredentials(mockFailureCollector);
    } catch (Exception e) {
      e.getMessage();
    }
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAndGetServiceAccountCredentialsWhenMockedGoogleCredentials() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      PowerMockito.spy(builder.setConnection(dataplexConnectorConfig).build());
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    try {
      dataplexBatchSinkConfig.validateAndGetServiceAccountCredentials(mockFailureCollector);
    } catch (Exception e) {
      e.getMessage();
    }
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenLocationIsNull() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(builder.build());
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenLocationIsNotNull() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(builder.setLocation("location").build());
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenGetLocationThrowsExceptionWith404() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(getBuilder().setLocation("test").build());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    ApiException apiException = new ApiException(new RuntimeException(), statusCode, false);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenLakeIsNotNull() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(builder.setLake("example-lake")
      .setLocation("test").build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigWhenLakeIsNotNullAndThrows404Exception() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    ApiException apiException = new ApiException(new RuntimeException(), statusCode, false);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      PowerMockito.spy(builder.setLake("example-lake").setLocation("test").build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenThrow(apiException);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());

  }

  @Test
  public void validateAssetConfigWhenLakeIsNotNullAndThrowsException() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    ApiException apiException = new ApiException(new RuntimeException(), statusCode, false);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(builder.setLocation("location").setLake(
      "example-lake").build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenThrow(apiException);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenZoneIsNotNull() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      PowerMockito.spy(builder.setLake("example-lake").setLocation("test").setZone("example-zone").build());
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigWhenZoneIsNotNullAndThrows404Exception() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    ApiException apiException = new ApiException(new RuntimeException(), statusCode, false);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      PowerMockito.spy(builder.setLake("example-lake").setLocation("test").setZone("example-zone").build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenThrow(apiException);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigWhenZoneIsNotNullAndThrowsException() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    ApiException apiException = new ApiException(new RuntimeException(), statusCode, false);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      PowerMockito.spy(builder.setLake("example-lake").setLocation("test").setZone("example-zone").build());

    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any()))
      .thenThrow(apiException);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenAssetIsNotNull() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    Asset asset = mock(Asset.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      PowerMockito.spy(builder.setLake("example-lake").setLocation("test").setZone("example-zone").
        setAsset("example-asset")
        .setAssetType(Asset.ResourceSpec.Type.BIGQUERY_DATASET.toString()).build());

    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any())).thenReturn(asset);
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.BIGQUERY_DATASET);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenAssetTypeIsNotEqual() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    Asset asset = mock(Asset.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      PowerMockito.spy(builder.setLake("example-lake").setLocation("test").setZone("example-zone")
        .setAsset("example-asset")
        .setAssetType(Asset.ResourceSpec.Type.BIGQUERY_DATASET.toString()).setFormat("csv").build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any())).thenReturn(asset);
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.STORAGE_BUCKET);
    try {
      dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    } catch (Exception e) {
      e.getMessage();
    }
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenCuratedZone() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    Asset asset = mock(Asset.class);
    Zone zone = PowerMockito.spy(Zone.newBuilder().setType(Zone.Type.CURATED).build());
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(builder.setLake("example-lake")
      .setLocation("test").setZone("example-zone")
      .setAsset("example-asset")
      .setAssetType(Asset.ResourceSpec.Type
        .BIGQUERY_DATASET.toString())
      .setFormat("json").build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(zone);
    when(dataplexServiceClient.getAsset((AssetName) any())).thenReturn(asset);
    when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.STORAGE_BUCKET);
    try {
      dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    } catch (Exception e) {
    }
    assertEquals(2, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigWhenAssetIsNotNullAndThrows404Exception() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    ApiException apiException = new ApiException(new RuntimeException(), statusCode, false);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      PowerMockito.spy(builder.setLake("example-lake").setLocation("test").setZone("example-zone").setAsset("example" +
          "-asset")
        .build());

    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any()))
      .thenThrow(apiException);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigWhenAssetIsNotNullAndThrowsException() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    ApiException apiException = new ApiException(new RuntimeException(), statusCode, false);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      PowerMockito.spy(builder.setLake("example-lake").setLocation("test").setZone("example-zone")
        .setAsset("example-asset")
        .build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any()))
      .thenThrow(apiException);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexServiceClient);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateBigQueryDatasetWhenGetAssetThrowsException() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Schema schema = mock(Schema.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(builder.setTable("test").build());
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    Mockito.when(dataplexServiceClient.getAsset(any(String.class)))
      .thenThrow(ApiException.class);
    dataplexBatchSinkConfig.validateBigQueryDataset(schema, schema, mockFailureCollector, dataplexServiceClient);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateBigQueryDatasetWhenNoMacro() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    GCPConnectorConfig dataplexConnectorConfig = PowerMockito.spy(new GCPConnectorConfig("",
      "",
      "",
      ""));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Schema schema = mock(Schema.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    Asset asset = mock(Asset.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(builder.setTable("test")
      .setConnection(dataplexConnectorConfig)
      .build());
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    Mockito.when(dataplexServiceClient.getAsset(any(String.class))).thenReturn(asset);
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getName()).thenReturn("projects/sap-adapter/datasets/exaple_lake_example_zone");
    PowerMockito.doNothing().when(dataplexBatchSinkConfig, "validateConfiguredSchema", any(), any(), any());
    dataplexBatchSinkConfig.validateBigQueryDataset(schema, schema, mockFailureCollector, dataplexServiceClient);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void autoServiceAccountUnavailableWhenConnectionIsNullWithMock() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    GCPConnectorConfig dataplexConnectorConfig = PowerMockito.spy(new GCPConnectorConfig("",
      "",
      "filePath",
      ""));
    DataplexBatchSinkConfig dataplexBatchSinkConfig = builder.setConnection(dataplexConnectorConfig).build();
    assertFalse(dataplexBatchSinkConfig.autoServiceAccountUnavailable());
  }

  @Test
  public void validateContentTypeWhenAvroFormatWithValidationError() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSinkConfig dataplexBatchSinkConfig = builder.setFormat("avro").build();
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateContentTypeWhenJsonFormatWithValidationError() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSinkConfig dataplexBatchSinkConfig = builder.setFormat("json").build();
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateContentTypeWhenCsvFormatWithValidationError() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSinkConfig dataplexBatchSinkConfig = builder.setFormat("csv").build();
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateContentTypeWhenParquetFormatWithValidationError() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSinkConfig dataplexBatchSinkConfig = builder.setFormat("parquet").build();
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateContentTypeWhenOrcFormatWithValidationError() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSinkConfig dataplexBatchSinkConfig = builder.setFormat("orc").build();
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateContentTypeWhenDefaultCase() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSinkConfig dataplexBatchSinkConfig = builder.setFormat("test").build();
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateStorageBucketTest() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(getBuilder().build());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PipelineConfigurer pipelineConfigurer = mock(PipelineConfigurer.class);
    PowerMockito.when(dataplexBatchSinkConfig.containsMacro(any())).thenReturn(true);
    dataplexBatchSinkConfig.validateStorageBucket(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateStorageBucketTestWTableNull() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(getBuilder().setTable(null).build());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PowerMockito.when(dataplexBatchSinkConfig.containsMacro(any())).thenReturn(false);
    try {
      dataplexBatchSinkConfig.validateStorageBucket(mockFailureCollector);
    } catch (ValidationException e) {
      assertEquals(1, mockFailureCollector.getValidationFailures().size());
    }
  }

  @Test
  public void validateStorageBucketOnFailures() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PipelineConfigurer pipelineConfigurer = mock(PipelineConfigurer.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(builder.build());
    PowerMockito.doNothing().when(dataplexBatchSinkConfig, "validateFormatForStorageBucket", any(), any());
    dataplexBatchSinkConfig = PowerMockito.spy(builder.setSuffix("test").setTable("table").build());
    when(dataplexBatchSinkConfig.getSchema(any())).thenThrow(IllegalArgumentException.class);
    dataplexBatchSinkConfig.validateStorageBucket(mockFailureCollector);
    assertEquals(2, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateStorageBucketOnSimpleDateFormatSuccess() throws Exception {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PipelineConfigurer pipelineConfigurer = mock(PipelineConfigurer.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(builder.build());
    PowerMockito.doNothing().when(dataplexBatchSinkConfig, "validateFormatForStorageBucket", any(), any());
    dataplexBatchSinkConfig = PowerMockito.spy(builder.setSuffix("yyyy-MM-dd").setTable("table").build());
    dataplexBatchSinkConfig.validateStorageBucket(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateFormatForStorageBucketWhenFormatIsNull() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    DataplexBatchSinkConfig dataplexBatchSinkConfig = builder.build();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PipelineConfigurer pipelineConfigurer = mock(PipelineConfigurer.class);
    try {
      dataplexBatchSinkConfig.validateFormatForStorageBucket(pipelineConfigurer, mockFailureCollector);
    } catch (Exception e) {
      e.getMessage();
    }
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateFormatForStorageBucketWhenFormatIsNotNull() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PipelineConfigurer pipelineConfigurer = mock(PipelineConfigurer.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig = builder.setFormat("csv").build();
    try {
      dataplexBatchSinkConfig.validateFormatForStorageBucket(pipelineConfigurer, mockFailureCollector);
    } catch (Exception e) {
      e.getMessage();
    }
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testConstructor() {
    DataplexBatchSinkConfig config = getBuilder().setLocation("location").setLake("lake").setFormat("avro").
      setTable("table").setZone("zone").setAsset("asset").setAssetType("assetType").setSuffix("suffix")
      .setReferenceName("referenceName").setOperation("insert").setDedupeBy("dedupeby").setTableKey("key")
      .setPartitioningType("time").setClusteringOrder("clusterOrder").setPartitionByField("partitionByField")
      .setRangeEnd(20L).setRangeInterval(1L).setRangeStart(0L).
      setUpdateDataplexMetadata(true).setRequirePartitionField(true).setPartitionFilter("partition").build();
    assertEquals("asset", config.getAsset());
    assertEquals("location", config.getLocation());
    assertEquals("lake", config.getLake());
    assertEquals("AVRO", config.getFormat().toString());
    assertEquals("table", config.getTable());
    assertEquals("assetType", config.getAssetType());
    assertEquals("zone", config.getZone());
    assertEquals("suffix", config.getSuffix());
    assertEquals("referenceName", config.getReferenceName(""));
    assertEquals("INSERT", config.getOperation().toString());
    assertEquals("dedupeby", config.getDedupeBy());
    assertEquals("key", config.getTableKey());
    assertEquals("TIME", config.getPartitioningType().toString());
    assertEquals("clusterOrder", config.getClusteringOrder());
    assertEquals("partitionByField", config.getPartitionByField());
    assertEquals("partition", config.getPartitionFilter());
    Assert.assertNull(config.getContentType("format"));
    Assert.assertNotNull(config.getRangeEnd());
    Assert.assertNotNull(config.getRangeInterval());
    Assert.assertNotNull(config.getRangeStart());
    Assert.assertTrue(config.isUpdateDataplexMetadata());
    Assert.assertTrue(config.isRequirePartitionField());
  }

  @Test
  public void testPartitionFilterNull() {
    DataplexBatchSinkConfig config = getBuilder().setPartitionFilter(null).build();
    Assert.assertNull(config.getPartitionFilter());
  }

  @Test
  public void testPartitionFilterWithClause() {
    DataplexBatchSinkConfig config = getBuilder().setPartitionFilter("WHERE filter").build();
    assertEquals(" filter", config.getPartitionFilter());
  }

  @Test
  public void testGetSchema() {
    MockFailureCollector failureCollector = new MockFailureCollector();
    String schema = "Schema.recordOf(\"record\",\n" +
      "                                    Schema.Field.of(\"id\", Schema.of(Schema.Type.LONG)));";
    DataplexBatchSinkConfig config = getBuilder().setSchema(schema).build();
    try {
      config.getSchema(failureCollector);
      Assert.fail("Exception is not thrown on valid schema");
    } catch (ValidationException e) {
      assertEquals(1, failureCollector.getValidationFailures().size());

    }
  }

  @Test
  public void testValidateOutputFormatProvider() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(builder.setTable("test").build());
    MockFailureCollector failureCollector = new MockFailureCollector();
    FormatContext context = Mockito.mock(FormatContext.class);
    String format = "format";
    ValidatingOutputFormat validatingOutputFormat = Mockito.mock(ValidatingOutputFormat.class);
    dataplexBatchSinkConfig.validateOutputFormatProvider(context, format, validatingOutputFormat);
    assertEquals(0, failureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateOutputFormatProviderWNull() {
    DataplexBatchSinkConfig.Builder builder = getBuilder();
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(builder.setTable("test").build());
    MockFailureCollector failureCollector = new MockFailureCollector();
    FormatContext context = Mockito.mock(FormatContext.class);
    Mockito.when(context.getFailureCollector()).thenReturn(failureCollector);
    String format = "format";
    try {
      dataplexBatchSinkConfig.validateOutputFormatProvider(context, format, null);
    } catch (ValidationException e) {
      assertEquals(1, failureCollector.getValidationFailures().size());
    }
  }

  @Test
  public void testValidateBigQueryDataset() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("order",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    Mockito.doNothing().when(dataplexServiceClient).close();
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(Boolean.TRUE);
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
    Mockito.doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
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
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBigQueryDatasetWithPartitionByFieldNull() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
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
    Mockito.doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
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
    dataplexBatchSinkConfig.validateBigQueryDataset(schema, schema, mockFailureCollector, dataplexServiceClient);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBigQueryDatasetWithMoreNumberofOrders() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("id",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
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
    Mockito.doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
      Mockito.any());
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any())).thenReturn(asset);
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.STORAGE_BUCKET);
    Mockito.when(assetResourceSpec.getName()).thenReturn("Project/dataset/table/entity");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField
      ("clusteringOrder"), "order, order1, order2, order3, order4");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField
      ("partitionByField"), "partitionByField");
    dataplexBatchSinkConfig.validateBigQueryDataset(schema, schema, mockFailureCollector, dataplexServiceClient);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBigQueryDatasetWithUnsupportedDatatype() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("order", io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.NULL)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
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
    Mockito.doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
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
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBigQueryDatasetWithUpdateOperation() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("order", io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.LONG)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
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
        setFormat("json").setLake("lake").setZone("zone").setLocation("location")
        .setAsset("asset").setOperation("update")
        .setAssetType((Asset.ResourceSpec.Type.STORAGE_BUCKET).toString()).build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    when(dataplexBatchSinkConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    Mockito.doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
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
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBigQueryDatasetWithTableKeyNotPresentInSchema() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("order", io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.LONG)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
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
        setOperation("upsert").setFormat("json").setLake("lake").setZone("zone").setLocation("location")
        .setAsset("asset").setTableKey("key").setAssetType((Asset.ResourceSpec.Type.STORAGE_BUCKET)
          .toString()).build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    when(dataplexBatchSinkConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    Mockito.doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
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
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBigQueryDatasetWithTableKey() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("order", io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.LONG)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("key", io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.STRING)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
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
        setOperation("upsert").setFormat("json").setLake("lake").setZone("zone").setLocation("location")
        .setAsset("asset").setTableKey("key").setAssetType((Asset.ResourceSpec.Type.STORAGE_BUCKET)
          .toString()).build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    when(dataplexBatchSinkConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    Mockito.doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
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
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBigQueryDatasetWithInvalidFieldPattern() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("order", io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.LONG)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("key", io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.LONG)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("id/", io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.LONG)));

    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
    Mockito.doNothing().when(dataplexServiceClient).close();
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    when(dataplexConnectorConfig.getServiceAccountType()).thenReturn("account");
    when(dataplexConnectorConfig.tryGetProject()).thenReturn("project");
    Asset asset = mock(Asset.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      Mockito.spy(DataplexBatchSinkConfig.builder()
        .setReferenceName("test").setAssetType(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE).
        setConnection(dataplexConnectorConfig).setUpdateDataplexMetadata(true).setTable("table").
        setOperation("upsert").setFormat("json").setLake("lake").setZone("zone").setLocation("location")
        .setAsset("asset").setTableKey("key").setAssetType((Asset.ResourceSpec.Type.STORAGE_BUCKET)
          .toString()).build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    when(dataplexBatchSinkConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    Mockito.doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
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
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBigQueryDatasetWithoutDedupeInSchema() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("order", io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.LONG)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("key", io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.LONG)));

    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
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
        setOperation("upsert").setFormat("json").setLake("lake").setZone("zone").setLocation("location")
        .setDedupeBy("dedupe")
        .setAsset("asset").setTableKey("key").setAssetType((Asset.ResourceSpec.Type.STORAGE_BUCKET)
          .toString()).build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    when(dataplexBatchSinkConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    Mockito.doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
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
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidateBigQueryDatasetWithDedupeInSchema() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("order", io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.LONG)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("key", io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.LONG)),
      io.cdap.cdap.api.data.schema.Schema.Field.of("dedupe",
        io.cdap.cdap.api.data.schema.Schema.of(Schema.Type.STRING)));

    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GCPConnectorConfig dataplexConnectorConfig = mock(GCPConnectorConfig.class);
    GoogleCredentials googleCredentials = Mockito.mock(GoogleCredentials.class);
    PowerMockito.mockStatic(DataplexUtil.class);
    DataplexServiceClient dataplexServiceClient = mock(DataplexServiceClient.class);
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
        setOperation("upsert").setFormat("json").setLake("lake").setZone("zone").setLocation("location")
        .setDedupeBy("dedupe")
        .setAsset("asset").setTableKey("key").setAssetType((Asset.ResourceSpec.Type.STORAGE_BUCKET)
          .toString()).build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    when(dataplexBatchSinkConfig.getSchema(mockFailureCollector)).thenReturn(schema);
    Mockito.doNothing().when(dataplexBatchSinkConfig).validateOutputFormatProvider(Mockito.any(), Mockito.anyString(),
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
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }


  // It will throw the validation errors for not defining the ranges.
  @Test
  public void testValidatePartitionProperties() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema =
      io.cdap.cdap.api.data.schema.Schema.recordOf("record", io.cdap.cdap.api.data.schema.Schema.Field.of("field",
        io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
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
    when(dataplexConnectorConfig.getServiceAccount()).thenReturn("serviceAccount");
    PowerMockito.mockStatic(BigQueryUtil.class);
    TableId tableId = Mockito.mock(TableId.class);
    Table table = Mockito.mock(Table.class);
    TimePartitioning timePartitioning = Mockito.mock(TimePartitioning.class);
    StandardTableDefinition tableDefinition = Mockito.mock(StandardTableDefinition.class);
    Mockito.when(table.getDefinition()).thenReturn(tableDefinition);
    Mockito.when(table.getTableId()).thenReturn(tableId);
    Mockito.when(tableDefinition.getTimePartitioning()).thenReturn(timePartitioning);
    Mockito.when(timePartitioning.getField()).thenReturn("field");
    Mockito.when(BigQueryUtil.getBigQueryTable(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyBoolean(), Mockito.any())).
      thenReturn(table);
    Asset asset = mock(Asset.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    // Assigning the values in DataplexBatchSinkConfig class
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      Mockito.spy(DataplexBatchSinkConfig.builder()
        .setReferenceName("test").setAssetType(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE).
        setConnection(dataplexConnectorConfig).setUpdateDataplexMetadata(true).setTable("table").
        setFormat("format").setLake("lake").setZone("zone").setLocation("location").setAsset("asset").
        setAssetType(Asset.ResourceSpec.Type.BIGQUERY_DATASET.toString()).setPartitionByField("field")
        .setPartitioningType("integer").build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any())).thenReturn(asset);
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.BIGQUERY_DATASET);
    Mockito.when(assetResourceSpec.getName()).thenReturn("Project/dataset/table/entity");
    when(dataplexBatchSinkConfig.getCredentials(mockFailureCollector)).thenReturn(googleCredentials);
    dataplexBatchSinkConfig.validateBigQueryDataset(schema, schema, mockFailureCollector, dataplexServiceClient);
    Assert.assertEquals(3, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidatePartitionPropertiesWithNegativeIntervalRange() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("field", io.cdap.cdap.api.data.schema.Schema.of(io.cdap.cdap.
        api.data.schema.Schema.Type.LONG)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
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
    when(dataplexConnectorConfig.getServiceAccount()).thenReturn("serviceAccount");
    PowerMockito.mockStatic(BigQueryUtil.class);
    TableId tableId = Mockito.mock(TableId.class);
    Table table = Mockito.mock(Table.class);
    TimePartitioning timePartitioning = Mockito.mock(TimePartitioning.class);
    StandardTableDefinition tableDefinition = Mockito.mock(StandardTableDefinition.class);
    Mockito.when(table.getDefinition()).thenReturn(tableDefinition);
    Mockito.when(table.getTableId()).thenReturn(tableId);
    Mockito.when(tableDefinition.getTimePartitioning()).thenReturn(timePartitioning);
    Mockito.when(timePartitioning.getField()).thenReturn("field");
    Mockito.when(BigQueryUtil.getBigQueryTable(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyBoolean(), Mockito.any())).
      thenReturn(table);
    Asset asset = mock(Asset.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    // Assigning the values in DataplexBatchSinkConfig class
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      Mockito.spy(DataplexBatchSinkConfig.builder()
        .setReferenceName("test").setAssetType(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE).
        setConnection(dataplexConnectorConfig).setUpdateDataplexMetadata(true).setTable("table").
        setFormat("format").setLake("lake").setZone("zone").setLocation("location").setAsset("asset").
        setAssetType(Asset.ResourceSpec.Type.BIGQUERY_DATASET.toString()).setPartitionByField("field").
        setPartitioningType("integer").setRangeStart(1L).setRangeInterval(-1L).setRangeEnd(2L)
        .setAllowSchemaRelaxation(true).build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any())).thenReturn(asset);
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.BIGQUERY_DATASET);
    Mockito.when(assetResourceSpec.getName()).thenReturn("Project/dataset/table/entity");
    when(dataplexBatchSinkConfig.getCredentials(mockFailureCollector)).thenReturn(googleCredentials);
    dataplexBatchSinkConfig.validateBigQueryDataset(schema, schema, mockFailureCollector, dataplexServiceClient);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidatePartitionPropertiesWithTimePartitioningNull() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("field", io.cdap.cdap.api.data.schema.Schema.
        of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
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
    when(dataplexConnectorConfig.getServiceAccount()).thenReturn("serviceAccount");
    PowerMockito.mockStatic(BigQueryUtil.class);
    TableId tableId = Mockito.mock(TableId.class);
    Table table = Mockito.mock(Table.class);
    TimePartitioning timePartitioning = Mockito.mock(TimePartitioning.class);
    RangePartitioning rangePartitioning = Mockito.mock(RangePartitioning.class);
    StandardTableDefinition tableDefinition = Mockito.mock(StandardTableDefinition.class);
    Mockito.when(table.getDefinition()).thenReturn(tableDefinition);
    Mockito.when(table.getTableId()).thenReturn(tableId);
    Mockito.when(tableDefinition.getTimePartitioning()).thenReturn(null);
    Mockito.when(tableDefinition.getRangePartitioning()).thenReturn(rangePartitioning);
    Mockito.when(rangePartitioning.getField()).thenReturn("rangeField");
    Mockito.when(BigQueryUtil.getBigQueryTable(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyBoolean(), Mockito.any())).
      thenReturn(table);
    Asset asset = mock(Asset.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    // Assigning the values in DataplexBatchSinkConfig class
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      Mockito.spy(DataplexBatchSinkConfig.builder()
        .setReferenceName("test").setAssetType(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE).
        setConnection(dataplexConnectorConfig).setUpdateDataplexMetadata(true).setTable("table").
        setFormat("format").setLake("lake").setZone("zone").setLocation("location").setAsset("asset").
        setAssetType(Asset.ResourceSpec.Type.BIGQUERY_DATASET.toString()).setPartitionByField("field").
        setPartitioningType("integer").setRangeStart(1L).setRangeInterval(1L).setRangeEnd(2L)
        .setAllowSchemaRelaxation(true).build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any())).thenReturn(asset);
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.BIGQUERY_DATASET);
    Mockito.when(assetResourceSpec.getName()).thenReturn("Project/dataset/table/entity");
    when(dataplexBatchSinkConfig.getCredentials(mockFailureCollector)).thenReturn(googleCredentials);
    dataplexBatchSinkConfig.validateBigQueryDataset(schema, schema, mockFailureCollector, dataplexServiceClient);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testValidatePartitionPropertiesWithTimePartitioningNullWithNonIntegerType() throws Exception {
    io.cdap.cdap.api.data.schema.Schema schema = io.cdap.cdap.api.data.schema.Schema.recordOf("record",
      io.cdap.cdap.api.data.schema.Schema.Field.of("field", io.cdap.cdap.api.data.schema.Schema.
        of(io.cdap.cdap.api.data.schema.Schema.Type.LONG)));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
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
    when(dataplexConnectorConfig.getServiceAccount()).thenReturn("serviceAccount");
    PowerMockito.mockStatic(BigQueryUtil.class);
    TableId tableId = Mockito.mock(TableId.class);
    Table table = Mockito.mock(Table.class);
    RangePartitioning rangePartitioning = Mockito.mock(RangePartitioning.class);
    StandardTableDefinition tableDefinition = Mockito.mock(StandardTableDefinition.class);
    Mockito.when(table.getDefinition()).thenReturn(tableDefinition);
    Mockito.when(table.getTableId()).thenReturn(tableId);
    Mockito.when(tableDefinition.getTimePartitioning()).thenReturn(null);
    Mockito.when(tableDefinition.getRangePartitioning()).thenReturn(rangePartitioning);
    Mockito.when(rangePartitioning.getField()).thenReturn("rangeField");
    Mockito.when(BigQueryUtil.getBigQueryTable(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(),
        Mockito.anyString(), Mockito.anyBoolean(), Mockito.any())).
      thenReturn(table);
    Asset asset = mock(Asset.class);
    Asset.ResourceSpec assetResourceSpec = mock(Asset.ResourceSpec.class);
    // Assigning the values in DataplexBatchSinkConfig class
    DataplexBatchSinkConfig dataplexBatchSinkConfig =
      Mockito.spy(DataplexBatchSinkConfig.builder()
        .setReferenceName("test").setAssetType(DataplexConstants.STORAGE_BUCKET_ASSET_TYPE).
        setConnection(dataplexConnectorConfig).setUpdateDataplexMetadata(true).setTable("table").
        setFormat("format").setLake("lake").setZone("zone").setLocation("location").setAsset("asset").
        setAssetType(Asset.ResourceSpec.Type.BIGQUERY_DATASET.toString()).setPartitionByField("field").
        setRangeStart(1L).setRangeInterval(1L).setRangeEnd(2L).setAllowSchemaRelaxation(true).build());
    doReturn("project").when(dataplexBatchSinkConfig).tryGetProject();
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials(mockFailureCollector);
    when(dataplexServiceClient.getLake((LakeName) any())).thenReturn(Lake.newBuilder().build());
    when(dataplexServiceClient.getZone((ZoneName) any())).thenReturn(Zone.newBuilder().build());
    when(dataplexServiceClient.getAsset((AssetName) any())).thenReturn(asset);
    Mockito.when(asset.getResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn(Asset.ResourceSpec.Type.BIGQUERY_DATASET);
    Mockito.when(assetResourceSpec.getName()).thenReturn("Project/dataset/table/entity");
    when(dataplexBatchSinkConfig.getCredentials(mockFailureCollector)).thenReturn(googleCredentials);
    dataplexBatchSinkConfig.validateBigQueryDataset(schema, schema, mockFailureCollector, dataplexServiceClient);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }
}
