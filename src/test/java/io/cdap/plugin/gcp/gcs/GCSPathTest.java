/*
 * Copyright © 2018 Cask Data, Inc.
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
package io.cdap.plugin.gcp.gcs;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link GCSPath}
 */
public class GCSPathTest {

  @Test
  public void testGetPath() {
    GCSPath gcsPath = GCSPath.from("gs://my-bucket/part1");
    Assert.assertEquals("gs://my-bucket/part1", "gs://" + gcsPath.getBucket() + gcsPath.getUri().getPath());
    gcsPath = GCSPath.from("my-bucket/part1");
    Assert.assertEquals("gs://my-bucket/part1", "gs://" + gcsPath.getBucket() + gcsPath.getUri().getPath());
    gcsPath = GCSPath.from("/my-bucket/part1");
    Assert.assertEquals("gs://my-bucket/part1", "gs://" + gcsPath.getBucket() + gcsPath.getUri().getPath());

    gcsPath = GCSPath.from("gs://my-bucket/part1/part2");
    Assert.assertEquals("gs://my-bucket/part1/part2", "gs://" + gcsPath.getBucket() + gcsPath.getUri().getPath());
    gcsPath = GCSPath.from("my-bucket/part1/part2");
    Assert.assertEquals("gs://my-bucket/part1/part2", "gs://" + gcsPath.getBucket() + gcsPath.getUri().getPath());
    gcsPath = GCSPath.from("/my-bucket/part1/part2");
    Assert.assertEquals("gs://my-bucket/part1/part2", "gs://" + gcsPath.getBucket() + gcsPath.getUri().getPath());
    gcsPath = GCSPath.from("gs://my-bucket/part1/hello world");
    Assert.assertEquals("gs://my-bucket/part1/hello world",
                        "gs://" + gcsPath.getBucket() + gcsPath.getUri().getPath());
    gcsPath = GCSPath.from("gs://my-bucket/hello world 1/hello world 2");
    Assert.assertEquals("gs://my-bucket/hello world 1/hello world 2",
                        "gs://" + gcsPath.getBucket() + gcsPath.getUri().getPath());

    assertFailure(() -> GCSPath.from(""));
    assertFailure(() -> GCSPath.from("gs:/abc/"));
    assertFailure(() -> GCSPath.from("gs:///abc/"));
    assertFailure(() -> GCSPath.from("gs://test space in bucket name/"));
    assertFailure(() -> GCSPath.from("file://abc/"));
  }

  @Test
  public void testGetBucket() {
    Assert.assertEquals("my-bucket", GCSPath.from("gs://my-bucket/part1 test").getBucket());
    Assert.assertEquals("my-bucket", GCSPath.from("my-bucket/part1").getBucket());
    Assert.assertEquals("my-bucket", GCSPath.from("/my-bucket/part1").getBucket());

    Assert.assertEquals("my-bucket", GCSPath.from("gs://my-bucket/part1/part2").getBucket());
    Assert.assertEquals("my-bucket", GCSPath.from("my-bucket/part1/part2").getBucket());
    Assert.assertEquals("my-bucket", GCSPath.from("/my-bucket/part1/part2").getBucket());

    assertFailure(() -> GCSPath.from(""));
  }

  @Test
  public void testSlashes() {
    for (String path : new String[] { "gs://b0/n0", "b0/n0", "/b0/n0" }) {
      GCSPath gcsPath = GCSPath.from(path);
      Assert.assertEquals("b0", gcsPath.getBucket());
      Assert.assertEquals("n0", gcsPath.getName());
    }

    for (String path : new String[] { "gs://b0/", "gs://b0", "/b0", "/b0/" }) {
      GCSPath gcsPath = GCSPath.from(path);
      Assert.assertEquals("b0", gcsPath.getBucket());
      Assert.assertTrue(gcsPath.getName().isEmpty());
    }
  }

  private void assertFailure(Runnable runnable) {
    try {
      runnable.run();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
