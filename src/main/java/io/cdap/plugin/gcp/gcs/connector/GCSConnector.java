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
import com.google.auth.Credentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Category;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.format.connector.AbstractFileConnector;
import io.cdap.plugin.format.connector.FileTypeDetector;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;
import io.cdap.plugin.gcp.gcs.source.GCSSource;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * GCS Connector
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(GCSConnector.NAME)
@Category("Google Cloud Platform")
@Description("Connector to browse and sample from Google Cloud Storage")
public class GCSConnector extends AbstractFileConnector<GCPConnectorConfig> {
  public static final String NAME = "GCS";
  static final String BUCKET_TYPE = "bucket";
  static final String DIRECTORY_TYPE = "directory";
  static final String FILE_TYPE = "file";
  static final String LAST_MODIFIED_KEY = "Last Modified";
  static final String SIZE_KEY = "Size";
  static final String FILE_TYPE_KEY = "File Type";

  private GCPConnectorConfig config;

  public GCSConnector(GCPConnectorConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void test(ConnectorContext context) throws ValidationException {
    String project = config.tryGetProject();
    FailureCollector failureCollector = context.getFailureCollector();
    if (project == null) {
      failureCollector
        .addFailure("Could not detect Google Cloud project id from the environment.", "Please specify a project id.")
        .withConfigProperty(GCPConnectorConfig.NAME_PROJECT);
    }

    Boolean isServiceAccountFilePath = config.isServiceAccountFilePath();
    if (isServiceAccountFilePath == null) {
      failureCollector.addFailure("Service account type is undefined.", "Must be `filePath` or `JSON`");
    }

    // no need to continue here as we are not able to continue validating
    if (!failureCollector.getValidationFailures().isEmpty()) {
      return;
    }

    Credentials credentials = null;
    try {
      credentials =
        GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(), config.isServiceAccountFilePath());
    } catch (IOException e) {
      failureCollector.addFailure(String.format("Service account provided is not valid: %s.", e.getMessage()),
                                  "Please provide a valid service account key.").withStacktrace(e.getStackTrace());
    }

    try {
      Storage storage = GCPUtils.getStorage(project, credentials);
      storage.list(Storage.BucketListOption.pageSize(1));
    } catch (Exception e) {
      failureCollector.addFailure(String.format("Not able to connect to GCS. Error: %s", e.getMessage()),
                                  "Please provide valid configs to connect to GCS").withStacktrace(e.getStackTrace());
    }
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest request) throws IOException {
    String path = request.getPath();
    int limit = request.getLimit() == null || request.getLimit() <= 0 ? Integer.MAX_VALUE : request.getLimit();
    if (isRoot(path)) {
      return browseBuckets(limit);
    }
    return browseBlobs(GCSPath.from(path), limit);
  }

  @Override
  protected String getFullPath(String path) {
    if (isRoot(path)) {
      return GCSPath.SCHEME;
    }
    return GCSPath.from(path).getUri().toString();
  }

  @Override
  protected Map<String, String> getFileSystemProperties(String path) {
    return GCPUtils.getFileSystemProperties(config, path, new HashMap<>());
  }

  @Override
  protected void setConnectorSpec(ConnectorSpecRequest request, ConnectorSpec.Builder builder) {
    super.setConnectorSpec(request, builder);
    builder.addRelatedPlugin(
      new PluginSpec(GCSSource.NAME, BatchSource.PLUGIN_TYPE,
                     ImmutableMap.of(
                       GCSSource.GCSSourceConfig.NAME_USE_CONNECTION, "true",
                       GCSSource.GCSSourceConfig.NAME_CONNECTION, request.getConnectionWithMacro(),
                       GCSSource.GCSSourceConfig.NAME_PATH, getFullPath(request.getPath()))));
  }

  private BrowseDetail browseBuckets(int limit) throws IOException {
    Storage storage = getStorage();
    Page<Bucket> buckets = storage.list();
    int count = 0;
    BrowseDetail.Builder builder = BrowseDetail.builder();
    for (Bucket bucket : buckets.iterateAll()) {
      // stop here so the request does not hang
      if (count >= limit) {
        break;
      }
      String name = bucket.getName();
      builder.addEntity(BrowseEntity.builder(name, name, BUCKET_TYPE).canBrowse(true).canSample(true).build());
      count++;
    }
    return builder.setTotalCount(count).build();
  }

  private BrowseDetail browseBlobs(GCSPath path, int limit) throws IOException {
    Storage storage = getStorage();
    String pathBlobName = path.getName();
    Page<Blob> blobs = storage.list(path.getBucket(), Storage.BlobListOption.currentDirectory(),
                                    Storage.BlobListOption.prefix(pathBlobName));
    int count = 0;
    BrowseDetail.Builder builder = BrowseDetail.builder();
    // entity for the path itself will also get returned in the result since this is a prefix search.
    BrowseEntity entityForPath = null;
    for (Blob blob : blobs.iterateAll()) {
      String blobName = blob.getName();

      // if count reaches limit, just break out here, so the request will not hang if there are millions of files
      if (count >= limit) {
        break;
      }

      boolean directory = blob.isDirectory();
      BrowseEntity.Builder entity =
        BrowseEntity.builder(new File(blobName).getName(), String.format("%s/%s", blob.getBucket(), blobName),
                             directory ? DIRECTORY_TYPE : FILE_TYPE).canBrowse(directory).canSample(directory);

      if (!directory) {
        entity.addProperty(SIZE_KEY, BrowseEntityPropertyValue.builder(
          String.valueOf(blob.getSize()), BrowseEntityPropertyValue.PropertyType.SIZE_BYTES).build());
        entity.addProperty(LAST_MODIFIED_KEY , BrowseEntityPropertyValue.builder(
          String.valueOf(blob.getUpdateTime()), BrowseEntityPropertyValue.PropertyType.TIMESTAMP_MILLIS).build());
        String fileType = FileTypeDetector.detectFileType(blobName);
        entity.addProperty(FILE_TYPE_KEY, BrowseEntityPropertyValue.builder(
          fileType, BrowseEntityPropertyValue.PropertyType.STRING).build());
        entity.canSample(FileTypeDetector.isSampleable(fileType));
      }

      // don't add it to result now
      if (blobName.equals(pathBlobName)) {
        entityForPath = entity.build();
        continue;
      }
      count++;
      builder.addEntity(entity.build());
    }

    // if the request blob is not null but count is 0, that means the blob itself is the only one returned,
    // return itself if the type is file
    if (entityForPath != null && count == 0 && entityForPath.getType().equals(FILE_TYPE)) {
      return builder.setTotalCount(1).addEntity(entityForPath).build();
    }
    return builder.setTotalCount(count).build();
  }

  private Storage getStorage() throws IOException {
    Boolean serviceAccountFilePath = config.isServiceAccountFilePath();
    if (serviceAccountFilePath == null) {
      throw new IllegalArgumentException("Service account type is undefined. Must be `filePath` or `JSON`");
    }

    Credentials credentials =
      GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(), serviceAccountFilePath);
    return GCPUtils.getStorage(config.getProject(), credentials);
  }

  private boolean isRoot(String path) {
    return path.isEmpty() || path.equals(GCSPath.ROOT_DIR);
  }
}
