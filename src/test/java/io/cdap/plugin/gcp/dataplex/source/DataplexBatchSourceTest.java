package io.cdap.plugin.gcp.dataplex.source;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.Table;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.dataplex.common.config.DataplexBaseConfig;
import io.cdap.plugin.gcp.dataplex.source.config.DataplexBatchSourceConfig;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.FieldSetter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Tests for DataplexBatchSource
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DataplexBatchSource.class, DataplexBatchSourceConfig.class, BigQueryUtil.class})
public class DataplexBatchSourceTest {
  @Mock
  private GCPConnectorConfig connection;

  @Test
  public void tryGetProjectTest() {
    DataplexBatchSourceConfig config = spy(new DataplexBatchSourceConfig());
    when(config.tryGetProject()).thenReturn(null);
    assertNull(config.tryGetProject());
    verify(config, times(1)).tryGetProject();
  }

  @Test
  public void validateBigQueryDatasetTest() throws IOException {
    DataplexBatchSourceConfig dataplexBatchSourceConfig = new DataplexBatchSourceConfig();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    mockStatic(BigQueryUtil.class);
    Table table = mock(Table.class);
    when(BigQueryUtil.getBigQueryTable(anyString(), anyString(), anyString(), anyString(), anyBoolean(), any(
      FailureCollector.class))).thenReturn(table);
    dataplexBatchSourceConfig.validateBigQueryDataset(mockFailureCollector, "projectId", "dataset", "table");
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccount() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig config = spy(new DataplexBatchSourceConfig());
    GCPConnectorConfig gcpConnectorConfig = spy(new GCPConnectorConfig("", "", "", ""));
    FieldSetter.setField(config, DataplexBaseConfig.class.getDeclaredField("connection"),
      gcpConnectorConfig);
    config.validateServiceAccount(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountWhenIsServiceAccountJsonFalse() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig config = spy(new DataplexBatchSourceConfig());
    when(connection.isServiceAccountJson()).thenReturn(Boolean.FALSE);
    GCPConnectorConfig gcpConnectorConfig = spy(new GCPConnectorConfig("", "", "", ""));
    FieldSetter.setField(config, DataplexBaseConfig.class.getDeclaredField("connection"),
      gcpConnectorConfig);
    config.validateServiceAccount(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountWhenServiceAccountFilePathIsNull() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig config = spy(new DataplexBatchSourceConfig());
    when(connection.getServiceAccountFilePath()).thenReturn(null);
    GCPConnectorConfig gcpConnectorConfig = spy(new GCPConnectorConfig("", "", "", ""));
    FieldSetter.setField(config, DataplexBaseConfig.class.getDeclaredField("connection"),
      gcpConnectorConfig);
    config.validateServiceAccount(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountIsServiceAccountJsonTrue() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig config = spy(new DataplexBatchSourceConfig());
    GCPConnectorConfig gcpConnectorConfig = spy(new GCPConnectorConfig("", "", "", ""));
    FieldSetter.setField(config, DataplexBaseConfig.class.getDeclaredField("connection"),
      gcpConnectorConfig);
    when(connection.isServiceAccountJson()).thenReturn(Boolean.TRUE);
    when(connection.getServiceAccountFilePath()).thenReturn(null);
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    doReturn(googleCredentials).when(config).getCredentials();
    config.validateServiceAccount(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountWhenIsServiceAccountJsonTrueAndFilePathIsNotNull() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig config = spy(new DataplexBatchSourceConfig());
    FieldSetter.setField(config, DataplexBaseConfig.class.getDeclaredField("connection"),
      connection);
    when(connection.isServiceAccountJson()).thenReturn(Boolean.FALSE);
    when(connection.getServiceAccountFilePath()).thenReturn("/filepath");
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    doReturn(googleCredentials).when(config).getCredentials();
    config.validateServiceAccount(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }
}
