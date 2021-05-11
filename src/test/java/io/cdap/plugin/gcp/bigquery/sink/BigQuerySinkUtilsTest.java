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

package io.cdap.plugin.gcp.bigquery.sink;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link BigQuerySinkUtils}
 */
public class BigQuerySinkUtilsTest {

  @Test
  public void testConfigureNullBucket() {
    Configuration configuration = new Configuration();

    BigQuerySinkUtils.configureBucket(configuration, null, "some-run-id");

    Assert.assertTrue(configuration.getBoolean("fs.gs.bucket.delete.enable", false));
    Assert.assertEquals("gs://some-run-id/some-run-id", configuration.get("fs.default.name"));
    Assert.assertTrue(configuration.getBoolean("fs.gs.impl.disable.cache", false));
    Assert.assertFalse(configuration.getBoolean("fs.gs.metadata.cache.enable", true));
  }

  @Test
  public void testConfigureBucket() {
    Configuration configuration = new Configuration();

    BigQuerySinkUtils.configureBucket(configuration, "some-bucket", "some-run-id");

    Assert.assertFalse(configuration.getBoolean("fs.gs.bucket.delete.enable", false));
    Assert.assertEquals("gs://some-bucket/some-run-id", configuration.get("fs.default.name"));
    Assert.assertTrue(configuration.getBoolean("fs.gs.impl.disable.cache", false));
    Assert.assertFalse(configuration.getBoolean("fs.gs.metadata.cache.enable", true));
  }
}
