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
 *
 */

package io.cdap.plugin.gcp.gcs.connector;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.mock.common.MockConnectorConfigurer;
import io.cdap.cdap.etl.mock.common.MockConnectorContext;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.common.TestEnvironment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


/**
 * GCS connector test.
 *
 * In order to run this test, the service account must have permission to create buckets and objects.
 */
public class GCSConnectorTest {
  private static TestEnvironment testEnvironment;
  private static String bucket;
  private static Storage storage;

  @BeforeClass
  public static void setupTestClass() throws Exception {
    testEnvironment = TestEnvironment.load();

    long now = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());
    bucket = String.format("gcs-conn-test-%d-%s", now, UUID.randomUUID());
    storage = GCPUtils.getStorage(testEnvironment.getProject(), testEnvironment.getCredentials());
    Assume.assumeFalse("The test bucket already exists.", storage.get(bucket) != null);
  }

  @Before
  public void setUp() {
    storage.create(BucketInfo.newBuilder(bucket).build());
  }

  @After
  public void tearDown() {
    if (storage == null) {
      return;
    }
    // delete all blob in bucket, otherwise it is not allowed to get deleted
    StorageBatch batch = storage.batch();
    Page<Blob> blobs = storage.list(bucket);
    for (Blob blob : blobs.iterateAll()) {
      batch.delete(blob.getBlobId());
    }
    batch.submit();
    storage.delete(bucket);
  }

  @Test
  public void testGCSConnector() throws Exception {
    // create data
    Bucket bkt = storage.get(bucket);
    List<BrowseEntity> entities = new ArrayList<>();

    for (int i = 0; i < 5; i++) {
      // file0.txt
      String blobName = "file" + i + ".txt";
      Blob blob = bkt.create(blobName, "Hello, World!".getBytes(StandardCharsets.UTF_8));
      entities.add(BrowseEntity.builder(blobName, bucket + "/" + blobName, GCSConnector.FILE_TYPE).canSample(true)
                     .setProperties(getFileProperties(blob)).build());

      // test0/text0.txt
      String folderName = "test" + i;
      // for some reason gcs does not have an api to create an empty folder
      blobName = folderName + "/text" + i + ".txt";
      // add folder
      entities.add(BrowseEntity.builder(folderName, bucket + "/" + folderName + "/", GCSConnector.DIRECTORY_TYPE)
                     .canSample(true).canBrowse(true).build());
      bkt.create(blobName, "Hello, World!".getBytes(StandardCharsets.UTF_8));
    }

    bkt.create("verifyempty/", new ByteArrayInputStream(new byte[0]));
    entities.add(BrowseEntity.builder("verifyempty", bucket + "/" + "verifyempty" + "/", GCSConnector.DIRECTORY_TYPE)
                   .canSample(true).canBrowse(true).build());

    entities.sort(Comparator.comparing(BrowseEntity::getName));
    testGCSConnector(new GCPConnectorConfig(testEnvironment.getProject(), GCPConnectorConfig.SERVICE_ACCOUNT_FILE_PATH,
                                            testEnvironment.getServiceAccountFilePath(), null), entities);
    if (testEnvironment.getServiceAccountContent() != null) {
      testGCSConnector(new GCPConnectorConfig(testEnvironment.getProject(), GCPConnectorConfig.SERVICE_ACCOUNT_JSON,
                                              null, testEnvironment.getServiceAccountContent()),
                       entities);
    }
  }

  private void testGCSConnector(GCPConnectorConfig config, List<BrowseEntity> entities) throws IOException {
    GCSConnector connector = new GCSConnector(new GCSConnectorConfig(
      config.getProject(), config.getServiceAccountType(), config.getServiceAccountFilePath(),
      config.getServiceAccountJson(), null));
    MockConnectorContext context = new MockConnectorContext(new MockConnectorConfigurer());
    connector.test(context);
    Assert.assertTrue(context.getFailureCollector().getValidationFailures().isEmpty());

    // browse bucket, here just check if it contains bucket since we don't know if there are other buckets
    BrowseDetail detail = connector.browse(context, BrowseRequest.builder("/").build());

    Assert.assertTrue(detail.getTotalCount() > 0);

    // browse bucket
    for (BrowseEntity entity : detail.getEntities()) {
      Assert.assertEquals(GCSConnector.BUCKET_TYPE, entity.getType());
      Assert.assertTrue(entity.canBrowse());
      Assert.assertTrue(entity.canSample());
    }

    // browse blob
    detail = connector.browse(context, BrowseRequest.builder("/" + bucket).build());
    BrowseDetail expected = BrowseDetail.builder().setTotalCount(11).setEntities(entities).build();
    assertBrowseDetailEquals(expected, detail);

    // browse limited
    detail = connector.browse(context, BrowseRequest.builder("/" + bucket).setLimit(5).build());
    expected = BrowseDetail.builder().setTotalCount(5).setEntities(entities.subList(0, 5)).build();
    assertBrowseDetailEquals(expected, detail);

    // browse one single file
    detail = connector.browse(context, BrowseRequest.builder("/" + bucket + "/" + "file0.txt").build());
    expected = BrowseDetail.builder().setTotalCount(1).setEntities(entities.subList(0, 1)).build();
    assertBrowseDetailEquals(expected, detail);

    // browse empty blob, should give empty list
    detail = connector.browse(context, BrowseRequest.builder("/" + bucket + "/" + "verifyempty/").build());
    expected = BrowseDetail.builder().setTotalCount(0).build();
    assertBrowseDetailEquals(expected, detail);
  }

  private Map<String, BrowseEntityPropertyValue> getFileProperties(Blob blob) {
    return ImmutableMap.of(
      GCSConnector.FILE_TYPE_KEY, BrowseEntityPropertyValue.builder(
        "text/plain", BrowseEntityPropertyValue.PropertyType.STRING).build(),
      GCSConnector.LAST_MODIFIED_KEY, BrowseEntityPropertyValue.builder(
        String.valueOf(blob.getUpdateTime()), BrowseEntityPropertyValue.PropertyType.TIMESTAMP_MILLIS).build(),
      GCSConnector.SIZE_KEY, BrowseEntityPropertyValue.builder(
        String.valueOf(blob.getSize()), BrowseEntityPropertyValue.PropertyType.SIZE_BYTES).build());
  }

  /**
   * Check browse details are equal while ignoring fields set by the AbstractFileConnector
   */
  private void assertBrowseDetailEquals(BrowseDetail expected, BrowseDetail actual) {
    // propertyHeaders from actual will include addition properties from AbstractFileConnector. Only compare the
    // ones added by the GCS connector
    Assert.assertTrue(actual.getPropertyHeaders().containsAll(expected.getPropertyHeaders()));

    // each entity from actual will contain additional properties, only compare the GCS ones
    Assert.assertEquals(expected.getTotalCount(), actual.getTotalCount());
    List<BrowseEntity> modifiedActualEntities = new ArrayList<>();
    List<String> gcsProperties = Arrays.asList(GCSConnector.LAST_MODIFIED_KEY,
                                               GCSConnector.SIZE_KEY,
                                               GCSConnector.FILE_TYPE_KEY);
    for (BrowseEntity actualEntity : actual.getEntities()) {
      Map<String, BrowseEntityPropertyValue> properties = new HashMap<>();
      for (String gcsProperty : gcsProperties) {
        if (actualEntity.getProperties().containsKey(gcsProperty)) {
          properties.put(gcsProperty, actualEntity.getProperties().get(gcsProperty));
        }
      }
      modifiedActualEntities.add(
        BrowseEntity.builder(actualEntity.getName(), actualEntity.getPath(), actualEntity.getType())
          .setProperties(properties)
          .canSample(actualEntity.canSample())
          .canBrowse(actualEntity.canBrowse())
          .build());
    }
    BrowseDetail modifiedActual = BrowseDetail.builder()
      .setTotalCount(actual.getTotalCount())
      // actual BrowseDetail contains sampleProperties that the AbstractFileConnector populates by looking at
      // @Description annotations in its config class. Too brittle to check this in unit test as descriptions may change
      .setSampleProperties(Collections.emptyList())
      .setEntities(modifiedActualEntities)
      .build();
    Assert.assertEquals(expected, modifiedActual);
  }
}
