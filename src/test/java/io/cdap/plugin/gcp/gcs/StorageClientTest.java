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
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Tests for storage client
 */
public class StorageClientTest {

  @Mock
  private Storage storage;

  private StorageClient storageClient;

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

  private final PrintStream originalOut = System.out;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    storageClient = new StorageClient(storage);
    System.setOut(new PrintStream(outContent));
  }

  @After
  public void restoreStreams() {
    System.setOut(originalOut);
  }

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
  public void testCreateBucketIfNotExists() {
    // Test successful bucket creation
    GCSPath path = GCSPath.from("my-bucket");
    storageClient.createBucketIfNotExists(path, "us", null);
    // No exception is thrown and method storage.create() is invoked once
    verify(storage, times(1)).create(any(BucketInfo.class));

    // Test bucket already exists
    GCSPath existingPath = GCSPath.from("existing-bucket");

    when(storage.create(any(BucketInfo.class))).thenThrow(new StorageException(409, "Conflict"));

    storageClient.createBucketIfNotExists(existingPath, "existing-location", null);
    // The exception thrown should be caught and warning log should be printed
    Assert.assertTrue(outContent.toString().contains("Getting 409 Conflict"));
    // The method storage.create() is invoked 2 times in total
    verify(storage, times(2)).create(any(BucketInfo.class));

    // Test bucket creation failure
    GCSPath failurePath = GCSPath.from("failed-bucket");

    when(storage.create(any(BucketInfo.class))).thenThrow(new StorageException(500, "Internal Server Error"));

    try {
      storageClient.createBucketIfNotExists(failurePath, "failed-location", null);
    } catch (Exception e) {
      // Verify that RuntimeException is caught
      if (!(e instanceof RuntimeException)) {
        Assert.fail(String.format("Test for detecting bucket creation failure did not succeed. " +
                                    "Unexpected Exception caught: %s", e));
      }
      // The method storage.create() is invoked 3 times in total
      verify(storage, times(3)).create(any(BucketInfo.class));
      return;
    }
    Assert.fail("Test for detecting bucket creation failure did not succeed. No exception caught");
  }
}
