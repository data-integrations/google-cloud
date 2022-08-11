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
import com.google.cloud.dataplex.v1.Asset;
import com.google.cloud.dataplex.v1.AssetName;
import com.google.cloud.dataplex.v1.DataplexServiceClient;
import com.google.cloud.dataplex.v1.Lake;
import com.google.cloud.dataplex.v1.LakeName;
import com.google.cloud.dataplex.v1.Zone;
import com.google.cloud.dataplex.v1.ZoneName;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
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
@PrepareForTest({DataplexBatchSinkConfig.class, Schema.class, DataplexServiceClient.class})
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
      .setLocation("test").setZone("example-zone").setAsset("example-asset")
      .setAssetType(Asset.ResourceSpec.Type.BIGQUERY_DATASET.toString()).setFormat("json").build());
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
        "-asset").build());

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
    GCPConnectorConfig dataplexConnectorConfig = PowerMockito.spy(new GCPConnectorConfig("", "", "", ""));
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
    GCPConnectorConfig dataplexConnectorConfig = PowerMockito.spy(new GCPConnectorConfig("", "", "filePath",
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

}
