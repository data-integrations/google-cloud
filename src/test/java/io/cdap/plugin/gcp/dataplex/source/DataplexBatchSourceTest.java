package io.cdap.plugin.gcp.dataplex.source;

import com.google.auth.oauth2.GoogleCredentials;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.dataplex.sink.connector.DataplexConnectorConfig;
import io.cdap.plugin.gcp.dataplex.source.config.DataplexBatchSourceConfig;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.FieldSetter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;

/**
 * Tests for DataplexBatchSource
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({DataplexBatchSource.class, DataplexBatchSourceConfig.class})
public class DataplexBatchSourceTest {

  @Mock
  private DataplexConnectorConfig connection;

  @Test
  public void tryGetProjectTest() {
    DataplexBatchSourceConfig config = spy(new DataplexBatchSourceConfig());
    when(config.tryGetProject()).thenReturn(null);
    assertNull(config.tryGetProject());
    verify(config, times(1)).tryGetProject();
  }

  @Test
  public void validateBigQueryDatasetTest() throws NoSuchFieldException {
    DataplexBatchSourceConfig dataplexBatchSourceConfig = new DataplexBatchSourceConfig();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    FieldSetter.setField(dataplexBatchSourceConfig, DataplexBatchSourceConfig.class.getDeclaredField("table"), "test");
    dataplexBatchSourceConfig.validateBigQueryDataset(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccount() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig config = spy(new DataplexBatchSourceConfig());
    DataplexConnectorConfig dataplexConnectorConfig = spy(new DataplexConnectorConfig("", "", "", ""));
    FieldSetter.setField(config, DataplexBatchSourceConfig.class.getDeclaredField("connection"),
      dataplexConnectorConfig);
    config.validateServiceAccount(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountWhenIsServiceAccountJsonFalse() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig config = spy(new DataplexBatchSourceConfig());
    when(connection.isServiceAccountJson()).thenReturn(Boolean.FALSE);
    DataplexConnectorConfig dataplexConnectorConfig = spy(new DataplexConnectorConfig("", "", "", ""));
    FieldSetter.setField(config, DataplexBatchSourceConfig.class.getDeclaredField("connection"),
      dataplexConnectorConfig);
    config.validateServiceAccount(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountWhenServiceAccountFilePathIsNull() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig config = spy(new DataplexBatchSourceConfig());
    when(connection.getServiceAccountFilePath()).thenReturn(null);
    DataplexConnectorConfig dataplexConnectorConfig = spy(new DataplexConnectorConfig("", "", "", ""));
    FieldSetter.setField(config, DataplexBatchSourceConfig.class.getDeclaredField("connection"),
      dataplexConnectorConfig);
    config.validateServiceAccount(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountIsServiceAccountJsonTrue() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig config = spy(new DataplexBatchSourceConfig());
    DataplexConnectorConfig dataplexConnectorConfig = spy(new DataplexConnectorConfig("", "", "", ""));
    FieldSetter.setField(config, DataplexBatchSourceConfig.class.getDeclaredField("connection"),
      dataplexConnectorConfig);
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
    FieldSetter.setField(config, DataplexBatchSourceConfig.class.getDeclaredField("connection"),
      connection);
    when(connection.isServiceAccountJson()).thenReturn(Boolean.FALSE);
    when(connection.getServiceAccountFilePath()).thenReturn("/filepath");
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    doReturn(googleCredentials).when(config).getCredentials();
    config.validateServiceAccount(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }
}
