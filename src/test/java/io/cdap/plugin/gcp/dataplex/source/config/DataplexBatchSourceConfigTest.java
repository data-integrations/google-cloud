package io.cdap.plugin.gcp.dataplex.source.config;

import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

/**
 * Tests for DataplexBatchSourceConfig
 */
@RunWith(MockitoJUnitRunner.class)
public class DataplexBatchSourceConfigTest {

  @InjectMocks
  DataplexBatchSourceConfig dataplexBatchSourceConfig;

  @Test
  public void testGetProject() {
    assertThrows(IllegalArgumentException.class, () -> (dataplexBatchSourceConfig).getProject());
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
  public void testIsTruncateTable() {
    assertFalse((dataplexBatchSourceConfig).isTruncateTable());
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
    assertNull((dataplexBatchSourceConfig).isServiceAccountFilePath());
  }

  @Test
  public void testGetSchema() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    assertNull(dataplexBatchSourceConfig.getSchema(mockFailureCollector));
  }

  @Test
  public void testValidateBigQueryDataset() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    dataplexBatchSourceConfig.validateBigQueryDataset(mockFailureCollector);
    PluginProperties expectedProperties = dataplexBatchSourceConfig.getRawProperties();
    assertEquals(expectedProperties.getProperties().size(), 0);
  }

  @Test
  public void testValidateTable() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    dataplexBatchSourceConfig.validateTable(mockFailureCollector);
    PluginProperties expectedProperties = dataplexBatchSourceConfig.getRawProperties();
    assertEquals(expectedProperties.getProperties().size(), 0);
  }
}

