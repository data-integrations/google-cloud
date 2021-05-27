/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.source;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;
import java.util.Date;

import static org.mockito.ArgumentMatchers.eq;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(GCPUtils.class)
public class BigQuerySourceUtilsTest {

  @Test
  public void getOrCreateBucket() throws InstantiationException, IllegalAccessException, NoSuchFieldException {
    System.out.println(new Date().getTime());
    PowerMockito.mockStatic(GCPUtils.class);

    Configuration configuration = new Configuration();
    BigQuerySourceConfig config = new BigQuerySourceConfig();
    Field dataset = BigQuerySourceConfig.class.getDeclaredField("dataset");
    dataset.setAccessible(true);
    dataset.set(config, "some-ds");

    Field connection = BigQuerySourceConfig.class.getDeclaredField("connection");
    connection.setAccessible(true);
    connection.set(config, new BigQueryConnectorConfig(null, null, null, null, null));

    Dataset ds = mock(Dataset.class);
    BigQuery bq = mock(BigQuery.class);
    when(bq.getDataset(eq("some-ds"))).thenReturn(ds);

    Credentials credentials = mock(Credentials.class);

    // Test with no bucket specified
    String bucket1 = BigQuerySourceUtils.getOrCreateBucket(configuration, config, bq, credentials, "some-path", null);
    Assert.assertEquals("bq-source-bucket-some-path", bucket1);

    // Test with a bucket specified
    Field bucket = BigQuerySourceConfig.class.getDeclaredField("bucket");
    bucket.setAccessible(true);
    bucket.set(config, "a-bucket");
    String bucket2 = BigQuerySourceUtils.getOrCreateBucket(configuration, config, bq, credentials, "some-path", null);
    Assert.assertEquals("a-bucket", bucket2);
  }
}
