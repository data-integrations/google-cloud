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

import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import io.cdap.cdap.api.exception.ProgramFailureException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class BigQuerySourceUtilsTest {

  @Test
  public void getOrCreateBucket() throws IllegalAccessException, NoSuchFieldException, IOException {
    Configuration configuration = new Configuration();
    BigQuerySourceConfig config = BigQuerySourceConfig.builder().build();

    Dataset ds = Mockito.mock(Dataset.class);
    Storage st = Mockito.mock(Storage.class);
    Bucket bkt = Mockito.mock(Bucket.class);
    Mockito.when(st.get(Mockito.anyString())).thenReturn(bkt);

    // Test with no bucket specified
    String bucket1 = BigQuerySourceUtils.getOrCreateBucket(configuration, st, config.getBucket(), ds,
                                                           "some-path", null);
    Assert.assertEquals("bq-source-bucket-some-path", bucket1);

    // Test with a bucket specified
    config = BigQuerySourceConfig.builder().setBucket("a-bucket").build();
    String bucket2 = BigQuerySourceUtils.getOrCreateBucket(configuration, st, config.getBucket(), ds,
                                                           "some-path", null);
    Assert.assertEquals("a-bucket", bucket2);
  }

  @Test
  public void getOrCreateBucket_nullBucketConflict409_returnsBucketAndEnablesDelete() throws IOException {
    Configuration configuration = new Configuration();
    BigQuerySourceConfig config = BigQuerySourceConfig.builder().build();
    Dataset ds = Mockito.mock(Dataset.class);
    Storage st = Mockito.mock(Storage.class);
    Mockito.when(st.create(Mockito.any(BucketInfo.class)))
      .thenThrow(new StorageException(409, "Your previous request to create the named bucket succeeded "
        + "and you already own it."));

    String bucket = BigQuerySourceUtils.getOrCreateBucket(configuration, st, config.getBucket(), ds,
                                                         "some-path", null);

    Assert.assertEquals("bq-source-bucket-some-path", bucket);
    Assert.assertTrue(configuration.getBoolean("fs.gs.bucket.delete.enable", false));
  }

  @Test
  public void getOrCreateBucket_nullBucketNon409Error_throwsProgramFailureException() {
    Configuration configuration = new Configuration();
    BigQuerySourceConfig config = BigQuerySourceConfig.builder().build();
    Dataset ds = Mockito.mock(Dataset.class);
    DatasetId datasetId = DatasetId.of("project", "dataset");
    Mockito.when(ds.getDatasetId()).thenReturn(datasetId);
    Mockito.when(ds.getLocation()).thenReturn("US");
    Storage st = Mockito.mock(Storage.class);
    Mockito.when(st.create(Mockito.any(BucketInfo.class)))
      .thenThrow(new StorageException(403, "Access denied"));

    ProgramFailureException exception = Assert.assertThrows(ProgramFailureException.class, () ->
      BigQuerySourceUtils.getOrCreateBucket(configuration, st, config.getBucket(), ds, "some-path", null));

    Assert.assertTrue(exception.getMessage().contains("Access denied"));
  }
}
