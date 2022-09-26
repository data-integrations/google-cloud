/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

/**
 * Tests for storage client
 */
public class StorageClientTest {

  @Test
  public void testAppend() {
    Assert.assertEquals("a/b/c", StorageClient.append("a/", "/b/c"));
    Assert.assertEquals("a/b/c", StorageClient.append("a", "b/c"));
    Assert.assertEquals("a/b/c", StorageClient.append("a/", "b/c"));
    Assert.assertEquals("a/b/c", StorageClient.append("a", "/b/c"));
    Assert.assertEquals("a/b/c", StorageClient.append("", "a/b/c"));
  }

  @Test
  public void testFileResolution() {
    Assert.assertEquals(BlobId.of("b0", "subdir/f1.txt"),
                        StorageClient.resolve("dir1/f1.txt", "dir1/f1.txt", GCSPath.from("b0/subdir"), true));
  }

  @Test
  public void testResolutionToExisting() {
    Assert.assertEquals(BlobId.of("b0", "subdir/dir2/a/b/c"),
                        StorageClient.resolve("dir1/dir2", "dir1/dir2/a/b/c", GCSPath.from("b0/subdir"), true));
  }

  @Test
  public void testResolutionToNonExisting() {
    Assert.assertEquals(BlobId.of("b0", "subdir/a/b/c"),
                        StorageClient.resolve("dir1/dir2", "dir1/dir2/a/b/c", GCSPath.from("b0/subdir"), false));
  }

  @Test
  public void testExistingDestinationEndingSlash() {
    Assert.assertEquals(BlobId.of("b0", "subdir/dir2/a/b/c"),
                        StorageClient.resolve("dir1/dir2", "dir1/dir2/a/b/c", GCSPath.from("b0/subdir"), true));
    Assert.assertEquals(BlobId.of("b0", "subdir/dir2/a/b/c"),
                        StorageClient.resolve("dir1/dir2", "dir1/dir2/a/b/c", GCSPath.from("b0/subdir/"), true));
  }

  @Test
  public void testNonExistingDestinationEndingSlash() {
    Assert.assertEquals(BlobId.of("b0", "subdir/a/b/c"),
                        StorageClient.resolve("dir1/dir2", "dir1/dir2/a/b/c", GCSPath.from("b0/subdir"), false));
    Assert.assertEquals(BlobId.of("b0", "subdir/dir2/a/b/c"),
                        StorageClient.resolve("dir1/dir2", "dir1/dir2/a/b/c", GCSPath.from("b0/subdir/"), false));
  }

  @Test
  public void testGetMatchingWildcardPath() {
    Assert.assertEquals(GCSPath.from("b0/test_1/sub_1")
      , StorageClient.getMatchingWildcardPath("test_1/sub_1/", "test_*/*", 1, true, "b0"));
    Assert.assertEquals(GCSPath.from("b0/test_1/sub_1")
      , StorageClient.getMatchingWildcardPath("test_1/sub_1/", "test_*/", 1, true, "b0"));
    Assert.assertEquals(null
      , StorageClient.getMatchingWildcardPath("test_1/sub_1/sub_2/", "test_*/*", 1, true, "b0"));
    Assert.assertEquals(GCSPath.from("b0/test_1/file.json")
      , StorageClient.getMatchingWildcardPath("test_1/file.json", "test_*/*.json", 1, true, "b0"));
    Assert.assertEquals(GCSPath.from("b0/test_1/file.json")
      , StorageClient.getMatchingWildcardPath("test_1/file.json", "test_*/*.json", 1, false, "b0"));
    Assert.assertEquals(GCSPath.from("b0/test_1/")
      , StorageClient.getMatchingWildcardPath("test_1/", "test_*/", 1, true, "b0"));
    Assert.assertEquals(GCSPath.from("b0/test_1/")
      , StorageClient.getMatchingWildcardPath("test_1/", "test_*/", 1, false, "b0"));
  }
}
