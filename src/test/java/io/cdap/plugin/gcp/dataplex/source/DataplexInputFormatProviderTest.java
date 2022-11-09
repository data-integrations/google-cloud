package io.cdap.plugin.gcp.dataplex.source;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataplex.v1.StorageSystem;
import io.cdap.plugin.common.batch.ConfigurationUtils;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexConstants;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.stream.StreamSupport;

import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ConfigurationUtils.class, GCPUtils.class, GoogleCredentials.class, DataplexUtil.class})
public class DataplexInputFormatProviderTest {

  @Mock
  JobContext mockContext;

  @Mock
  private Configuration configuration;

  @Mock
  private InputSplit inputSplit;

  @Mock
  private TaskAttemptContext context;

  @Test
  public void testGetInputFormatClassName() throws Exception {
    PowerMockito.mockStatic(StreamSupport.class);
    String entityType = StorageSystem.CLOUD_STORAGE.toString();
    when(configuration.get(DataplexConstants.DATAPLEX_ENTITY_TYPE)).thenReturn(entityType);
    PowerMockito.mockStatic(ConfigurationUtils.class);
    DataplexInputFormatProvider.DataplexInputFormat dataInputFormat = new
      DataplexInputFormatProvider.DataplexInputFormat();
    Map<String, String> map = new HashMap<>();
    map.put("price", "100");
    Mockito.when(ConfigurationUtils.getNonDefaultConfigurations(configuration)).thenReturn(map);
    DataplexInputFormatProvider dataplexInputFormatProvider = new DataplexInputFormatProvider(configuration);
    Assert.assertThrows(ClassCastException.class, () -> {
      dataInputFormat.createRecordReader(inputSplit, context);
    });
    Assert.assertEquals("io.cdap.plugin.gcp.dataplex.source.DataplexInputFormatProvider" +
      "$DataplexInputFormat", dataplexInputFormatProvider.getInputFormatClassName());
  }

  @Test
  public void testGetInputFormatDataplexClassNameWithBigquery() throws Exception {
    DataplexInputFormatProvider.DataplexInputFormat dataplexInputFormat = new
      DataplexInputFormatProvider.DataplexInputFormat();
    PowerMockito.mockStatic(StreamSupport.class);
    Spliterator spliterator = Mockito.mock(Spliterator.class);
    String entityType = "BIGQUERY";
    when(configuration.get(DataplexConstants.DATAPLEX_ENTITY_TYPE)).thenReturn(entityType);
    when(configuration.spliterator()).thenReturn(spliterator);
    DataplexInputFormatProvider dataplexInputFormatProvider = new DataplexInputFormatProvider(configuration);
    dataplexInputFormatProvider.getInputFormatConfiguration();
    Assert.assertThrows(IOException.class, () -> {
      dataplexInputFormat.getSplits(mockContext);
    });
    Assert.assertEquals("io.cdap.plugin.gcp.bigquery.source.PartitionedBigQueryInputFormat",
      dataplexInputFormatProvider.getInputFormatClassName());
  }

  @Test
  public void testGetInputFormatNotDataPlexClassNameNull() throws Exception {
    String entityType = "entity";
    when(configuration.get(DataplexConstants.DATAPLEX_ENTITY_TYPE)).thenReturn(entityType);
    DataplexInputFormatProvider dataplexInputFormatProvider = new DataplexInputFormatProvider(configuration);
    Assert.assertNull(dataplexInputFormatProvider.getInputFormatClassName());
  }

}
