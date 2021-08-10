package io.cdap.plugin.gcp.dataplex.sink.connector;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.connector.BrowseDetail;
import io.cdap.cdap.etl.api.connector.BrowseEntity;
import io.cdap.cdap.etl.api.connector.BrowseRequest;
import io.cdap.cdap.etl.api.connector.Connector;
import io.cdap.cdap.etl.api.connector.ConnectorContext;
import io.cdap.cdap.etl.api.connector.ConnectorSpec;
import io.cdap.cdap.etl.api.connector.ConnectorSpecRequest;
import io.cdap.cdap.etl.api.connector.DirectConnector;
import io.cdap.cdap.etl.api.connector.PluginSpec;
import io.cdap.cdap.etl.api.connector.SampleRequest;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.gcp.dataplex.sink.DataplexBatchSink;
import io.cdap.plugin.gcp.dataplex.sink.config.DataplexBaseConfig;
import io.cdap.plugin.gcp.dataplex.sink.connection.DataplexInterface;
import io.cdap.plugin.gcp.dataplex.sink.connection.out.DataplexInterfaceImpl;
import io.cdap.plugin.gcp.dataplex.sink.enums.AssetType;
import io.cdap.plugin.gcp.dataplex.sink.enums.ConnectorObject;
import io.cdap.plugin.gcp.dataplex.sink.model.Location;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(DataplexConnector.class);
    private DataplexInterface dataplexInterface = new DataplexInterfaceImpl();
    private DataplexConnectorConfig config;

    DataplexConnector(DataplexConnectorConfig config) {
        this.config = config;
    }


    @Override
    public void test(ConnectorContext connectorContext) throws ValidationException {
        //no-op
    }

    @Override
    public BrowseDetail browse(ConnectorContext connectorContext, BrowseRequest browseRequest) throws IOException {
        DataplexPath path = new DataplexPath(browseRequest.getPath());
        String location = path.getLocation();
        if (location == null) {
            return listLocations(path, browseRequest.getLimit());
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
        BrowseDetail.Builder builder = BrowseDetail.builder();
        builder.addEntity(BrowseEntity.builder(asset, asset, "Asset").canBrowse(false).canSample(true).build());
        return builder.setTotalCount(1).build();
    }

    private BrowseDetail listLocations(DataplexPath path, Integer limit) {
        int countLimit = limit == null || limit <= 0 ? Integer.MAX_VALUE : limit;
        int count = 0;
        BrowseDetail.Builder builder = BrowseDetail.builder();
        List<Location> locationList = dataplexInterface.listLocations(config.tryGetProject());
        for (Location location : locationList) {
            if (count >= countLimit) {
                break;
            }
            String name = location.toString();
            builder.addEntity(
              BrowseEntity.builder("/" + name,  name, ConnectorObject.Location.toString())
                .canSample(true).build());
            count++;
        }
        return builder.setTotalCount(count).build();
    }

    private BrowseDetail listLakes(DataplexPath path, Integer limit) {
        BrowseDetail.Builder builder = BrowseDetail.builder();
        String name = "lakes";
        builder.addEntity(BrowseEntity.builder(name, "/" + name, "Lake").canBrowse(true).canSample(true).build());
        return builder.setTotalCount(1).build();
    }

    private BrowseDetail listZones(DataplexPath path, Integer limit) {
        BrowseDetail.Builder builder = BrowseDetail.builder();
        String parentPath = String.format("/%s/", path.getLake());
        String name = "zones";
        builder.addEntity(BrowseEntity.builder(name, parentPath + name, "Zone").canBrowse(true).
                canSample(true).build());
        return builder.setTotalCount(1).build();
    }

    private BrowseDetail listAssets(DataplexPath path, Integer limit) {
        BrowseDetail.Builder builder = BrowseDetail.builder();
        String parentPath = String.format("/%s/%s/%s/", path.getLocation(), path.getLake(), path.getZone());
        String name = "assets";
        builder.addEntity(BrowseEntity.builder(name, parentPath + name, "Asset").
                canSample(true).build());
        return builder.setTotalCount(1).build();
    }

    @Override
    public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest)
      throws IOException {
        ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
        DataplexPath path = new DataplexPath(connectorSpecRequest.getPath());
        Map<String, String> properties = new HashMap<>();
        properties.put(DataplexBaseConfig.NAME_LAKE, path.getLake());
        properties.put(DataplexBaseConfig.NAME_ZONE, path.getZone());
        properties.put(DataplexBaseConfig.NAME_ASSET, path.getAsset());
        properties.put(DataplexBaseConfig.NAME_ASSET_TYPE, AssetType.STORAGE_BUCKET.toString());
        return specBuilder.addRelatedPlugin(new PluginSpec(DataplexBatchSink.NAME, BatchSink.PLUGIN_TYPE, properties))
          .build();
    }

    @Override
    public List<StructuredRecord> sample(ConnectorContext connectorContext, SampleRequest sampleRequest)
      throws IOException {
        return Collections.emptyList();
    }
}
