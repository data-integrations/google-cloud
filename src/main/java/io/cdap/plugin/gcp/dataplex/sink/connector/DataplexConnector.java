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

package io.cdap.plugin.gcp.dataplex.sink.connector;

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseEntityPropertyValue;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.format.connector.FileTypeDetector;
import io.cdap.plugin.gcp.bigquery.source.BigQuerySourceConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dataplex.common.config.DataplexBaseConfig;
import io.cdap.plugin.gcp.dataplex.sink.DataplexBatchSink;
import io.cdap.plugin.gcp.dataplex.sink.connection.DataplexInterface;
import io.cdap.plugin.gcp.dataplex.sink.connection.out.DataplexInterfaceImpl;
import io.cdap.plugin.gcp.dataplex.sink.exception.ConnectorException;
import io.cdap.plugin.gcp.dataplex.sink.model.Asset;
import io.cdap.plugin.gcp.dataplex.sink.model.Lake;
import io.cdap.plugin.gcp.dataplex.sink.model.Location;
import io.cdap.plugin.gcp.dataplex.sink.model.Zone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dataplex Connector Plugin
 */
@Plugin(type = Connector.PLUGIN_TYPE)
@Name(DataplexConnector.NAME)
@Description("This connector enables browsing feature to fetch the locations, lakes, zones and assets information " +
  "from Dataplex.")
public class DataplexConnector implements DirectConnector {
  public static final String NAME = "Dataplex";
  static final String DIRECTORY_TYPE = "directory";
  static final String FILE_TYPE = "file";
  static final String LAST_MODIFIED_KEY = "Last Modified";
  static final String SIZE_KEY = "Size";
  static final String FILE_TYPE_KEY = "File Type";
  private static final String DATAPLEX_LOCATION = "location";
  private static final String DATAPLEX_LAKE = "lake";
  private static final String BIGQUERY_DATASET = "BIGQUERY_DATASET";
  private static final String STORAGE_BUCKET = "STORAGE_BUCKET";
  private static final String DISPLAY_NAME = "Display Name";
  private static final Map<String, String> ASSET_MAP = ImmutableMap.of(BIGQUERY_DATASET, "BigQuery Dataset",
    STORAGE_BUCKET, "Storage Bucket");
  private static final int ERROR_CODE_NOT_FOUND = 404;

  private static final Logger LOG = LoggerFactory.getLogger(DataplexConnector.class);
  private static final DataplexInterface dataplexInterface = new DataplexInterfaceImpl();

  private final DataplexConnectorConfig config;

  DataplexConnector(DataplexConnectorConfig config) {
    this.config = config;
  }

  @Override
  public void test(ConnectorContext context) throws ValidationException {
    FailureCollector failureCollector = context.getFailureCollector();
    // validate project ID
    String project = config.tryGetProject();
    if (project == null) {
      failureCollector
        .addFailure("Could not detect Google Cloud project id from the environment.",
          "Please specify a project id.");
    }
    GoogleCredentials credentials = null;
    try {
      credentials = config.getCredentials();
    } catch (Exception e) {
      failureCollector.addFailure(String.format("Service account key provided is not valid: %s.",
        e.getMessage()), "Please provide a valid service account key.");
    }
    // if either project or credentials cannot be loaded , no need to continue
    if (!failureCollector.getValidationFailures().isEmpty()) {
      return;
    }

    try {
      dataplexInterface.listLocations(credentials,
        config.tryGetProject());
    } catch (Exception e) {
      failureCollector.addFailure(String.format("Could not connect to Dataplex: %s", e.getMessage()),
        "Please specify correct connection properties.");
    }
  }

  @Override
  public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest browseRequest) throws IOException {
    DataplexPath path = new DataplexPath(browseRequest.getPath());

    try {
      String location = path.getLocation();
      if (location == null) {
        return listLocations(browseRequest.getLimit());
      }

      String lake = path.getLake();
      if (lake == null) {
        return listLakes(path, browseRequest.getLimit());
      }

      String zone = path.getZone();
      if (zone == null) {
        return listZones(path, browseRequest.getLimit());
      }

      String asset = path.getAsset();
      if (asset == null) {
        return listAssets(path, browseRequest.getLimit());
      }
      Asset assetBean = dataplexInterface.getAsset(config.getCredentials(),
        config.tryGetProject(), path.getLocation(), path.getLake(), path.getZone(), path.getAsset());
      String assetType = assetBean.getAssetResourceSpec().getType();
      String objectName = path.getObjectName();
      if (BIGQUERY_DATASET.equalsIgnoreCase(assetType) && Strings.isNullOrEmpty(objectName)) {
        return listTables(path, browseRequest.getLimit());
      }

      if (STORAGE_BUCKET.equalsIgnoreCase(assetType)) {
        return browseBlobs(path, browseRequest.getLimit());
      }
    } catch (ConnectorException e) {
      LOG.debug(String.format("%s: %s", e.getCode(), e.getMessage()));
    }
    return BrowseDetail.builder().setTotalCount(0).build();
  }

  private BrowseDetail listLocations(Integer limit) throws IOException, ConnectorException {
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    int count = 0;
    BrowseDetail.Builder builder = BrowseDetail.builder();
    List<Location> locationList = dataplexInterface.listLocations(config.getCredentials(),
      config.tryGetProject());
    for (Location location : locationList) {
      if (count >= countLimit) {
        break;
      }
      builder.addEntity(
        BrowseEntity.builder(location.getLocationId(), "/" + location.getLocationId(), DATAPLEX_LOCATION)
          .canSample(true).canBrowse(true).build());
      count++;
    }
    return builder.setTotalCount(count).build();
  }


  private BrowseDetail listLakes(DataplexPath path, Integer limit) throws IOException, ConnectorException {
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    int count = 0;
    BrowseDetail.Builder builder = BrowseDetail.builder();
    String parentPath = String.format("/%s/", path.getLocation());
    List<Lake> lakeList = dataplexInterface.listLakes(config.getCredentials(),
      config.tryGetProject(), path.getLocation());
    for (Lake lake : lakeList) {
      if (count >= countLimit) {
        break;
      }
      BrowseEntity.Builder entity =
        BrowseEntity.builder(getObjectId(lake.getName()), parentPath + getObjectId(lake.getName()), DATAPLEX_LAKE)
          .canBrowse(true).canSample(true);
      count++;
      entity.addProperty(DISPLAY_NAME, BrowseEntityPropertyValue.builder(
        lake.displayName, BrowseEntityPropertyValue.PropertyType.STRING).build());
      builder.addEntity(entity.build());
    }
    return builder.setTotalCount(count).build();
  }


  private BrowseDetail listZones(DataplexPath path, Integer limit) throws IOException, ConnectorException {
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    int count = 0;
    BrowseDetail.Builder builder = BrowseDetail.builder();
    String parentPath = String.format("/%s/%s/", path.getLocation(), path.getLake());
    List<Zone> zonelist = dataplexInterface.listZones(config.getCredentials(),
      config.tryGetProject(), path.getLocation(), path.getLake());
    for (Zone zone : zonelist) {
      if (count >= countLimit) {
        break;
      }
      BrowseEntity.Builder entity =
        BrowseEntity.builder(getObjectId(zone.getName()), parentPath + getObjectId(zone.getName()),
          zone.getType().toLowerCase())
          .canBrowse(true).canSample(true);
      count++;
      entity.addProperty(DISPLAY_NAME, BrowseEntityPropertyValue.builder(
        zone.displayName, BrowseEntityPropertyValue.PropertyType.STRING).build());
      builder.addEntity(entity.build());
    }
    return builder.setTotalCount(count).build();
  }

  private BrowseDetail listAssets(DataplexPath path, Integer limit) throws IOException, ConnectorException {
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    int count = 0;
    BrowseDetail.Builder builder = BrowseDetail.builder();
    String parentPath = String.format("/%s/%s/%s/", path.getLocation(), path.getLake(), path.getZone());
    List<Asset> assetlist = dataplexInterface.listAssets(config.getCredentials(),
      config.tryGetProject(), path.getLocation(), path.getLake(), path.getZone());
    for (Asset asset : assetlist) {
      if (count >= countLimit) {
        break;
      }
      String assetType = asset.getAssetResourceSpec().getType();
      BrowseEntity.Builder entity =
        BrowseEntity.builder(getObjectId(asset.getName()),
          parentPath + getObjectId(asset.getName()),
          ASSET_MAP.get(assetType)).canSample(true).canBrowse(true);
      entity.addProperty(DISPLAY_NAME, BrowseEntityPropertyValue.builder(
        asset.displayName, BrowseEntityPropertyValue.PropertyType.STRING).build());
      builder.addEntity(entity.build());
      count++;
    }
    return builder.setTotalCount(count).build();
  }


  private BrowseDetail listTables(DataplexPath path, Integer limit) throws IOException, ConnectorException {
    int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
    int count = 0;
    BrowseDetail.Builder builder = BrowseDetail.builder();
    Asset dataplexAsset = dataplexInterface.getAsset(config.getCredentials(),
      config.tryGetProject(), path.getLocation(), path.getLake(), path.getZone(), path.getAsset());
    String[] assetValues = dataplexAsset.getAssetResourceSpec().name.split("/");
    String dataset = assetValues[assetValues.length - 1];
    String datasetProject = assetValues[assetValues.length - 3];
    String parentPath = String.format("/%s/%s/%s/%s/", path.getLocation(), path.getLake(), path.getZone(),
      path.getAsset());
    DatasetId datasetId = DatasetId.of(datasetProject, dataset);
    Page<Table> tablePage;
    try {
      tablePage = getBigQuery(config.getProject()).listTables(datasetId);
    } catch (BigQueryException e) {
      if (e.getCode() == ERROR_CODE_NOT_FOUND) {
        throw new IllegalArgumentException(String.format("Cannot find dataset: %s.", dataset), e);
      }
      throw e;
    }
    for (Table table : tablePage.iterateAll()) {
      if (count >= countLimit) {
        break;
      }
      BrowseEntity.Builder entity =
        BrowseEntity.builder(table.getTableId().getTable(), parentPath + table.getTableId().getTable(),
          table.getDefinition().getType().name().toLowerCase())
          .canSample(true);
      builder.addEntity(entity.build());
      count++;

    }
    return builder.setTotalCount(count).build();
  }


  private BrowseDetail browseBlobs(DataplexPath path, int limit) throws ConnectorException, IOException {
    Storage storage = getStorage();
    String pathBlobName = path.getObjectName() == null ? "" : path.getObjectName();
    Asset dataplexAsset = dataplexInterface.getAsset(config.getCredentials(),
      config.tryGetProject(), path.getLocation(), path.getLake(), path.getZone(), path.getAsset());
    String bucketName = dataplexAsset.getAssetResourceSpec().getName().
      substring(dataplexAsset.getAssetResourceSpec().getName().lastIndexOf('/') + 1);

    Page<Blob> blobs = storage.list(bucketName, Storage.BlobListOption.currentDirectory(),
      Storage.BlobListOption.prefix(pathBlobName));
    int count = 0;
    BrowseDetail.Builder builder = BrowseDetail.builder();
    // entity for the path itself will also get returned in the result since this is a prefix search.
    BrowseEntity entityForPath = null;
    String parentPath = String.format("/%s/%s/%s/%s/%s/", path.getLocation(), path.getLake(), path.getZone(),
      path.getAsset(), bucketName);
    for (Blob blob : blobs.iterateAll()) {
      String blobName = blob.getName();

      // if count reaches limit, just break out here, so the request will not hang if there are millions of files
      if (count >= limit) {
        break;
      }

      boolean directory = blob.isDirectory();
      BrowseEntity.Builder entity =
        BrowseEntity.builder(new File(blobName).getName(), parentPath + blobName,
          directory ? DIRECTORY_TYPE : FILE_TYPE).canBrowse(directory).canSample(directory);

      if (!directory) {
        entity.addProperty(SIZE_KEY, BrowseEntityPropertyValue.builder(
          String.valueOf(blob.getSize()), BrowseEntityPropertyValue.PropertyType.SIZE_BYTES).build());
        entity.addProperty(LAST_MODIFIED_KEY, BrowseEntityPropertyValue.builder(
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

  @Override
  public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest)
    throws IOException {
    ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
    DataplexPath path = new DataplexPath(connectorSpecRequest.getPath());

    Map<String, String> properties = new HashMap<>();
    properties.put(ConfigUtil.NAME_USE_CONNECTION, "true");
    properties.put(ConfigUtil.NAME_CONNECTION, connectorSpecRequest.getConnectionWithMacro());
    properties.put(DataplexBaseConfig.NAME_LOCATION, path.getLocation());
    properties.put(DataplexBaseConfig.NAME_LAKE, path.getLake());
    properties.put(DataplexBaseConfig.NAME_ZONE, path.getZone());
    properties.put(DataplexBaseConfig.NAME_ASSET, path.getAsset());
    Asset asset = null;
    try {
      asset = dataplexInterface.getAsset(config.getCredentials(),
        config.tryGetProject(), path.getLocation(), path.getLake(), path.getZone(), path.getAsset());
    } catch (ConnectorException e) {
      LOG.debug(String.format("%s: %s", e.getCode(), e.getMessage()));
    }
    String assetType = asset.getAssetResourceSpec().type;
    properties.put(DataplexBaseConfig.NAME_ASSET_TYPE, assetType);
    if (BIGQUERY_DATASET.equalsIgnoreCase(assetType)) {
      String[] assetValues = asset.getAssetResourceSpec().name.split("/");
      String dataset = assetValues[assetValues.length - 1];
      String datasetProject = assetValues[assetValues.length - 3];
      String tableName = path.getObjectName();
      if (tableName != null) {
        properties.put(BigQuerySourceConfig.NAME_TABLE, tableName);
        Table table = getTable(getBigQuery(config.getProject()), datasetProject, dataset, tableName);
        TableDefinition definition = table.getDefinition();
        Schema schema = BigQueryUtil.getTableSchema(definition.getSchema(), null);
        specBuilder.setSchema(schema);
        if (definition.getType() != TableDefinition.Type.TABLE) {
          properties.put(BigQuerySourceConfig.NAME_ENABLE_QUERYING_VIEWS, "true");
        }
      }
    } else if (STORAGE_BUCKET.equalsIgnoreCase(assetType)) {
      // to-do handling schema for GCS resources
    }
    return specBuilder.addRelatedPlugin(new PluginSpec(DataplexBatchSink.NAME, BatchSink.PLUGIN_TYPE, properties))
      .build();
  }

  @Override
  public List<StructuredRecord> sample(ConnectorContext connectorContext, SampleRequest sampleRequest)
    throws IOException {
    return Collections.emptyList();
  }

  private String getObjectId(String name) {
    return name.substring(name.lastIndexOf('/') + 1);
  }


  /**
   * Get BigQuery client
   *
   * @param project the GCP project where BQ dataset is listed and BQ job is run
   */
  private BigQuery getBigQuery(String project) throws IOException {
    GoogleCredentials credentials = null;
    //validate service account
    if (config.isServiceAccountJson() || config.getServiceAccountFilePath() != null) {
      credentials =
        GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(), config.isServiceAccountFilePath());
    }
    // Here project decides where the BQ job is run and under which the datasets is listed
    return GCPUtils.getBigQuery(project, credentials);
  }

  private Table getTable(BigQuery bigQuery, String datasetProject, String datasetName, String tableName) {
    Table table = bigQuery.getTable(TableId.of(datasetProject, datasetName, tableName));
    if (table == null) {
      throw new IllegalArgumentException(String.format("Cannot find tableName: %s.%s.", datasetName, tableName));
    }
    return table;
  }

  private Storage getStorage() throws IOException {
    Boolean serviceAccountFilePath = config.isServiceAccountFilePath();
    if (serviceAccountFilePath == null) {
      throw new IllegalArgumentException("Service account type is undefined. Must be `filePath` or `JSON`");
    }

    Credentials credentials =
      config.getServiceAccount() == null ? null :
        GCPUtils.loadServiceAccountCredentials(config.getServiceAccount(), serviceAccountFilePath);
    return GCPUtils.getStorage(config.getProject(), credentials);
  }
}
