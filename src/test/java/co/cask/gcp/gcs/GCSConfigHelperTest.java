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
package co.cask.gcp.gcs;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link GCSConfigHelper}
 */
public class GCSConfigHelperTest {

  @Test
  public void testGetPath() {
    Assert.assertEquals("gs://my-bucket/part1", GCSConfigHelper.getPath("gs://my-bucket/part1").toString());
    Assert.assertEquals("gs://my-bucket/part1", GCSConfigHelper.getPath("my-bucket/part1").toString());

    Assert.assertEquals("gs://my-bucket/part1/part2", GCSConfigHelper.getPath("gs://my-bucket/part1/part2").toString());
    Assert.assertEquals("gs://my-bucket/part1/part2", GCSConfigHelper.getPath("my-bucket/part1/part2").toString());

    assertFailure("");
    assertFailure("gs:/abc/");
    assertFailure("gs:///abc/");
    assertFailure("file://abc/");
  }

  private void assertFailure(String path) {
    try {
      GCSConfigHelper.getPath(path);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testGetBucket() {
    Assert.assertEquals("my-bucket", GCSConfigHelper.getBucket("gs://my-bucket/part1"));
    Assert.assertEquals("my-bucket", GCSConfigHelper.getBucket("my-bucket/part1"));

    Assert.assertEquals("my-bucket", GCSConfigHelper.getBucket("gs://my-bucket/part1/part2"));
    Assert.assertEquals("my-bucket", GCSConfigHelper.getBucket("my-bucket/part1/part2"));

    try {
      GCSConfigHelper.getBucket("");
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
