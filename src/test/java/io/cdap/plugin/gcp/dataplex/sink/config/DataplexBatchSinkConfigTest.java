package io.cdap.plugin.gcp.dataplex.sink.config;

import com.google.auth.oauth2.GoogleCredentials;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.dataplex.common.config.DataplexBaseConfig;
import io.cdap.plugin.gcp.dataplex.sink.connection.DataplexInterface;
import io.cdap.plugin.gcp.dataplex.sink.connection.out.DataplexInterfaceImpl;
import io.cdap.plugin.gcp.dataplex.sink.connector.DataplexConnectorConfig;
import io.cdap.plugin.gcp.dataplex.sink.exception.ConnectorException;
import io.cdap.plugin.gcp.dataplex.sink.model.Asset;
import io.cdap.plugin.gcp.dataplex.sink.model.Lake;
import io.cdap.plugin.gcp.dataplex.sink.model.Location;
import io.cdap.plugin.gcp.dataplex.sink.model.Zone;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({DataplexBatchSinkConfig.class, Schema.class, DataplexInterface.class})
public class DataplexBatchSinkConfigTest {

  @Test
  public void validateBigQueryDatasetTest() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = new DataplexBatchSinkConfig();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("table"), "test");
    dataplexBatchSinkConfig.validateBigQueryDataset(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateBigQueryDatasetWhenTableIsNull() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = new DataplexBatchSinkConfig();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("table"), null);
    try {
      dataplexBatchSinkConfig.validateBigQueryDataset(mockFailureCollector);
    } catch (Exception e) {
      e.printStackTrace();
    }
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateBigQueryDatasetWhenTruncateTableIsNotNull() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = new DataplexBatchSinkConfig();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("table"), "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("truncateTable"),
      new Boolean(Boolean.TRUE));
    dataplexBatchSinkConfig.validateBigQueryDataset(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateBigQueryDatasetWhenTruncateTableAndOperationIsNotNull() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = new DataplexBatchSinkConfig();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("table"), "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("truncateTable"),
      new Boolean(Boolean.TRUE));
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("operation"),
      "UPDATE");
    dataplexBatchSinkConfig.validateBigQueryDataset(mockFailureCollector);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountWhenIsServiceAccountJsonTrue() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = new DataplexBatchSinkConfig();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexConnectorConfig dataplexConnectorConfig = mock(DataplexConnectorConfig.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("connection"),
      dataplexConnectorConfig);
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    try {
      dataplexBatchSinkConfig.validateServiceAccount(mockFailureCollector);
    } catch (Exception e) {
      e.getMessage();
    }
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountWhenIsServiceAccountJsonTrueAndFilePathIsNotNull() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = new DataplexBatchSinkConfig();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexConnectorConfig dataplexConnectorConfig = mock(DataplexConnectorConfig.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("connection"),
      dataplexConnectorConfig);
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    try {
      dataplexBatchSinkConfig.validateServiceAccount(mockFailureCollector);
    } catch (Exception e) {
      e.getMessage();
    }
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountWhenMockedGoogleCredentials() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexConnectorConfig dataplexConnectorConfig = mock(DataplexConnectorConfig.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("connection"),
      dataplexConnectorConfig);
    when(dataplexConnectorConfig.isServiceAccountJson()).thenReturn(new Boolean(Boolean.TRUE));
    when(dataplexConnectorConfig.getServiceAccountFilePath()).thenReturn("test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    try {
      dataplexBatchSinkConfig.validateServiceAccount(mockFailureCollector);
    } catch (Exception e) {
      e.getMessage();
    }
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenLocationIsNull() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = new DataplexBatchSinkConfig();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterface = mock(DataplexInterface.class);

    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");

    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterface);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenLocationIsNotNull() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenReturn(new Location());
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenGetLocationThrowsExceptionWith404() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    ConnectorException connectorException = PowerMockito.spy(new ConnectorException("404", "error message"));
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenThrow(connectorException);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenGetLocationThrowsException() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenThrow(ConnectorException.class);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenLakeIsNotNull() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("lake"), "example lake");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenReturn(new Location());
    PowerMockito.when(dataplexInterfaceImpl.getLake(any(), any(), any(), any())).thenReturn(new Lake());
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigWhenLakeIsNotNullAndThrows404Exception() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    ConnectorException connectorException = PowerMockito.spy(new ConnectorException("404", "error message"));
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("lake"), "example lake");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenReturn(new Location());
    PowerMockito.when(dataplexInterfaceImpl.getLake(any(), any(), any(), any())).thenThrow(connectorException);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigWhenLakeIsNotNullAndThrowsException() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("lake"), "example lake");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenReturn(new Location());
    PowerMockito.when(dataplexInterfaceImpl.getLake(any(), any(), any(), any())).thenThrow(ConnectorException.class);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenZoneIsNotNull() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("lake"), "example lake");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("zone"), "example zone");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenReturn(new Location());
    PowerMockito.when(dataplexInterfaceImpl.getLake(any(), any(), any(), any())).thenReturn(new Lake());
    PowerMockito.when(dataplexInterfaceImpl.getZone(any(), any(), any(), any(), any())).thenReturn(new Zone());
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigWhenZoneIsNotNullAndThrows404Exception() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    ConnectorException connectorException = PowerMockito.spy(new ConnectorException("404", "error message"));
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("lake"), "example lake");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("zone"), "example zone");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenReturn(new Location());
    PowerMockito.when(dataplexInterfaceImpl.getLake(any(), any(), any(), any())).thenReturn(new Lake());
    PowerMockito.when(dataplexInterfaceImpl.getZone(any(), any(), any(), any(), any())).thenThrow(connectorException);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigWhenZoneIsNotNullAndThrowsException() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("lake"), "example lake");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("zone"), "example zone");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenReturn(new Location());
    PowerMockito.when(dataplexInterfaceImpl.getLake(any(), any(), any(), any())).thenReturn(new Lake());
    PowerMockito.when(dataplexInterfaceImpl.getZone(any(), any(), any(), any(), any()))
      .thenThrow(ConnectorException.class);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenAssetIsNotNull() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    Asset asset = mock(Asset.class);
    Asset.AssetResourceSpec assetResourceSpec = mock(Asset.AssetResourceSpec.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("lake"), "example lake");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("zone"), "example zone");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("asset"), "example asset");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("assetType"),
      "BIGQUERY_DATASET");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenReturn(new Location());
    PowerMockito.when(dataplexInterfaceImpl.getLake(any(), any(), any(), any())).thenReturn(new Lake());
    PowerMockito.when(dataplexInterfaceImpl.getZone(any(), any(), any(), any(), any())).thenReturn(new Zone());
    PowerMockito.when(dataplexInterfaceImpl.getAsset(any(), any(), any(), any(), any(), any())).thenReturn(asset);
    Mockito.when(asset.getAssetResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn("BIGQUERY_DATASET");
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenAssetTypeIsNotEqual() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    Asset asset = mock(Asset.class);
    Asset.AssetResourceSpec assetResourceSpec = mock(Asset.AssetResourceSpec.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("lake"), "example lake");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("zone"), "example zone");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("asset"), "example asset");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("assetType"), "BIGQUERY");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("format"), "format");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenReturn(new Location());
    PowerMockito.when(dataplexInterfaceImpl.getLake(any(), any(), any(), any())).thenReturn(new Lake());
    PowerMockito.when(dataplexInterfaceImpl.getZone(any(), any(), any(), any(), any())).thenReturn(new Zone());
    PowerMockito.when(dataplexInterfaceImpl.getAsset(any(), any(), any(), any(), any(), any())).thenReturn(asset);
    Mockito.when(asset.getAssetResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn("STORAGE_BUCKET");
    try {
      dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    } catch (Exception e) {
      e.getMessage();
    }
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigurationWhenCurratedZone() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    Asset asset = mock(Asset.class);
    Zone zone = spy(new Zone());
    Asset.AssetResourceSpec assetResourceSpec = mock(Asset.AssetResourceSpec.class);
    zone.setType("CURATED");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("lake"), "example lake");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("zone"), "example zone");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("asset"), "example asset");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("assetType"), "BIGQUERY");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("format"), "json");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenReturn(new Location());
    PowerMockito.when(dataplexInterfaceImpl.getLake(any(), any(), any(), any())).thenReturn(new Lake());
    PowerMockito.when(dataplexInterfaceImpl.getZone(any(), any(), any(), any(), any())).thenReturn(zone);
    PowerMockito.when(dataplexInterfaceImpl.getAsset(any(), any(), any(), any(), any(), any())).thenReturn(asset);
    Mockito.when(asset.getAssetResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getType()).thenReturn("STORAGE_BUCKET");
    try {
      dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    } catch (Exception e) {
      e.getMessage();
    }
    assertEquals(2, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigWhenAssetIsNotNullAndThrows404Exception() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    ConnectorException connectorException = PowerMockito.spy(new ConnectorException("404", "error message"));
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("lake"), "example lake");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("zone"), "example zone");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("asset"), "example asset");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenReturn(new Location());
    PowerMockito.when(dataplexInterfaceImpl.getLake(any(), any(), any(), any())).thenReturn(new Lake());
    PowerMockito.when(dataplexInterfaceImpl.getZone(any(), any(), any(), any(), any())).thenReturn(new Zone());
    PowerMockito.when(dataplexInterfaceImpl.getAsset(any(), any(), any(), any(), any(), any()))
      .thenThrow(connectorException);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateAssetConfigWhenAssetIsNotNullAndThrowsException() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("referenceName"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("lake"), "example lake");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("zone"), "example zone");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("asset"), "example asset");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBaseConfig.class.getDeclaredField("location"),
      "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    PowerMockito.when(dataplexInterfaceImpl.getLocation(any(), any(), any())).thenReturn(new Location());
    PowerMockito.when(dataplexInterfaceImpl.getLake(any(), any(), any(), any())).thenReturn(new Lake());
    PowerMockito.when(dataplexInterfaceImpl.getZone(any(), any(), any(), any(), any())).thenReturn(new Zone());
    PowerMockito.when(dataplexInterfaceImpl.getAsset(any(), any(), any(), any(), any(), any()))
      .thenThrow(ConnectorException.class);
    dataplexBatchSinkConfig.validateAssetConfiguration(mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateBigQueryDatasetWhenGetAssetThrowsException() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Schema schema = mock(Schema.class);
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("table"), "test");
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    Mockito.when(dataplexInterfaceImpl.getAsset(any(), any(), any(), any(), any(), any()))
      .thenThrow(ConnectorException.class);
    dataplexBatchSinkConfig.validateBigQueryDataset(schema, schema, mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateBigQueryDatasetWhenNoMacro() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    DataplexConnectorConfig dataplexConnectorConfig = PowerMockito.spy(new DataplexConnectorConfig("", "", "", ""));
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    Schema schema = mock(Schema.class);
    DataplexInterface dataplexInterfaceImpl = mock(DataplexInterfaceImpl.class);
    Asset asset = mock(Asset.class);
    Asset.AssetResourceSpec assetResourceSpec = mock(Asset.AssetResourceSpec.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("table"), "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("connection"),
      dataplexConnectorConfig);
    doReturn(googleCredentials).when(dataplexBatchSinkConfig).getCredentials();
    Mockito.when(dataplexInterfaceImpl.getAsset(any(), any(), any(), any(), any(), any())).thenReturn(asset);
    Mockito.when(asset.getAssetResourceSpec()).thenReturn(assetResourceSpec);
    Mockito.when(assetResourceSpec.getName()).thenReturn("projects/sap-adapter/datasets/exaple_lake_example_zone");
    PowerMockito.doNothing().when(dataplexBatchSinkConfig, "validateConfiguredSchema", any(), any(), any());
    dataplexBatchSinkConfig.validateBigQueryDataset(schema, schema, mockFailureCollector, dataplexInterfaceImpl);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void autoServiceAccountUnavailableWhenConnectionIsNull() {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    assertTrue(dataplexBatchSinkConfig.autoServiceAccountUnavailable());
  }

  @Test
  public void autoServiceAccountUnavailableWhenConnectionIsNullWithMock() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    DataplexConnectorConfig dataplexConnectorConfig = PowerMockito.spy(new DataplexConnectorConfig("", "", "filePath",
      ""));
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("connection"),
      dataplexConnectorConfig);
    assertFalse(dataplexBatchSinkConfig.autoServiceAccountUnavailable());
  }

  @Test
  public void validateContentTypeWhenAvroFormatWithValidationError() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("format"),
      "avro");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("contentType"),
      "application/avros");
    dataplexBatchSinkConfig.validateContentType(mockFailureCollector);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateContentTypeWhenJsonFormatWithValidationError() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("format"),
      "json");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("contentType"),
      "application/jsons");
    dataplexBatchSinkConfig.validateContentType(mockFailureCollector);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateContentTypeWhenCsvFormatWithValidationError() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("format"),
      "csv");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("contentType"),
      "application/csvs");
    dataplexBatchSinkConfig.validateContentType(mockFailureCollector);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateContentTypeWhenParquetFormatWithValidationError() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("format"),
      "parquet");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("contentType"),
      "application/parquets");
    dataplexBatchSinkConfig.validateContentType(mockFailureCollector);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateContentTypeWhenOrcFormatWithValidationError() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("format"),
      "orc");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("contentType"),
      "application/orcs");
    dataplexBatchSinkConfig.validateContentType(mockFailureCollector);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateContentTypeWhenDefaultCase() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("format"),
      "test");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("contentType"),
      "application/csv");
    dataplexBatchSinkConfig.validateContentType(mockFailureCollector);
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateStorageBucketTest() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PipelineConfigurer pipelineConfigurer = mock(PipelineConfigurer.class);
    PowerMockito.when(dataplexBatchSinkConfig.containsMacro(any())).thenReturn(true);
    dataplexBatchSinkConfig.validateStorageBucket(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateStorageBucketOnFailures() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PipelineConfigurer pipelineConfigurer = mock(PipelineConfigurer.class);
    PowerMockito.doNothing().when(dataplexBatchSinkConfig, "validateFormatForStorageBucket", any(), any());
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("suffix"),
      "test");
    when(dataplexBatchSinkConfig.getSchema(any())).thenThrow(IllegalArgumentException.class);
    dataplexBatchSinkConfig.validateStorageBucket(mockFailureCollector);
    assertEquals(2, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateStorageBucketOnSimpleDateFormatSuccess() throws Exception {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PipelineConfigurer pipelineConfigurer = mock(PipelineConfigurer.class);
    PowerMockito.doNothing().when(dataplexBatchSinkConfig, "validateFormatForStorageBucket", any(), any());
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("suffix"),
      "yyyy-MM-dd");
    dataplexBatchSinkConfig.validateStorageBucket(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateFormatForStorageBucketWhenFormatIsNull() {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
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
  public void validateFormatForStorageBucketWhenFormatIsNotNull() throws NoSuchFieldException {
    DataplexBatchSinkConfig dataplexBatchSinkConfig = PowerMockito.spy(new DataplexBatchSinkConfig());
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    PipelineConfigurer pipelineConfigurer = mock(PipelineConfigurer.class);
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("format"),
      "csv");
    FieldSetter.setField(dataplexBatchSinkConfig, DataplexBatchSinkConfig.class.getDeclaredField("contentType"),
      "other");
    try {
      dataplexBatchSinkConfig.validateFormatForStorageBucket(pipelineConfigurer, mockFailureCollector);
    } catch (Exception e) {
      e.getMessage();
    }
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

}
