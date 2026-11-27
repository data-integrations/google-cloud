/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigtable.common;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import io.cdap.plugin.gcp.bigtable.sink.BigtableOutputFormat;
import io.cdap.plugin.gcp.bigtable.source.BigtableInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Verifies that Cloud Bigtable plugins resolve HBase classes from hbase-shaded-server without
 * hbase-shaded-client or its shaded Jetty 6 classes on the classpath.
 */
public class BigtableShadedJarPackagingTest {

  @Test
  public void testHBaseShadedServerWithoutShadedJetty() throws Exception {
    File hbaseJar = new File(Put.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    Assert.assertTrue("Expected Put from hbase-shaded-server, got: " + hbaseJar.getName(),
                      hbaseJar.getName().startsWith("hbase-shaded-server-"));

    Assert.assertThrows(
      ClassNotFoundException.class,
      () -> Class.forName("org.apache.hadoop.hbase.shaded.org.mortbay.util.LazyList",
                          false, BigtableShadedJarPackagingTest.class.getClassLoader()));

    List<String> forbiddenEntries = new ArrayList<>();
    try (ZipFile zipFile = new ZipFile(hbaseJar)) {
      Enumeration<? extends ZipEntry> entries = zipFile.entries();
      while (entries.hasMoreElements()) {
        String name = entries.nextElement().getName();
        if (name.startsWith("org/apache/hadoop/hbase/shaded/org/mortbay/")
          || name.startsWith("META-INF/maven/org.mortbay.jetty/")
          || name.startsWith("META-INF/maven/org.eclipse.jetty/")) {
          forbiddenEntries.add(name);
        }
      }
    }
    Assert.assertTrue("Unexpected shaded Jetty entries: " + forbiddenEntries, forbiddenEntries.isEmpty());

    Configuration conf = HBaseConfiguration.create();
    BigtableConfiguration.configure(conf, "test-project", "test-instance");

    byte[] family = "cf".getBytes(StandardCharsets.UTF_8);
    byte[] qualifier = "col".getBytes(StandardCharsets.UTF_8);
    Put put = new Put("row-1".getBytes(StandardCharsets.UTF_8));
    put.addColumn(family, qualifier, "val".getBytes(StandardCharsets.UTF_8));
    Assert.assertTrue(put.has(family, qualifier));

    Scan scan = new Scan().addColumn(family, qualifier);
    String encodedScan = TableMapReduceUtil.convertScanToString(scan);
    Assert.assertFalse(encodedScan.isEmpty());

    Assert.assertNotNull(new BigtableInputFormat());
    Assert.assertNotNull(new BigtableOutputFormat());
  }
}
