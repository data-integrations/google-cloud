package io.cdap.plugin.gcp.dataplex.sink.connector;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.connector.*;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.plugin.gcp.dataplex.sink.DataplexBatchSink;
import io.cdap.plugin.gcp.dataplex.sink.config.DataplexBaseConfig;
import io.cdap.plugin.gcp.dataplex.sink.enums.AssetType;
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
@Description("This connector enables browsing feature to fetch the lakes, zones and assets information from Dataplex.")
public class DataplexConnector implements DirectConnector {
    public static final String NAME = "Dataplex";
    private static final Logger LOG = LoggerFactory.getLogger(DataplexConnector.class);

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
        String lake = path.getLake();
        if (lake == null) {
            return listLakes(10);
        }
        String zone = path.getZone();
        if (zone == null) {
            return listZones(10, lake);
        }
        String asset = path.getAsset();
        if (asset == null) {
            return listAssets(10, lake, zone);
        }
        BrowseDetail.Builder builder = BrowseDetail.builder();
        builder.addEntity(BrowseEntity.builder(asset, asset, "Asset").canBrowse(false).canSample(true).build());
        return builder.setTotalCount(1).build();
    }

    private BrowseDetail listLakes(Integer limit) {
        BrowseDetail.Builder builder = BrowseDetail.builder();
        String name = "lakes";
        builder.addEntity(BrowseEntity.builder(name, "/" + name, "Lake").canBrowse(true).canSample(true).build());
        return builder.setTotalCount(1).build();
    }

    private BrowseDetail listZones(Integer limit, String lake) {
        BrowseDetail.Builder builder = BrowseDetail.builder();
        String parentPath = String.format("/%s/", lake);;
        String name = "zones";
        builder.addEntity(BrowseEntity.builder(name, parentPath + name, "Zone").canBrowse(true).
                canSample(true).build());
        return builder.setTotalCount(1).build();
    }

    private BrowseDetail listAssets(Integer limit, String lake, String zone) {
        BrowseDetail.Builder builder = BrowseDetail.builder();
        String parentPath = String.format("/%s/%s/", lake, zone);
        String name = "assets";
        builder.addEntity(BrowseEntity.builder(name, parentPath + name, "Asset").
                canSample(true).build());
        return builder.setTotalCount(1).build();
    }

    @Override
    public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest)
      throws IOException {
        ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
        Map<String, String> properties = new HashMap<>();
        properties.put(DataplexBaseConfig.NAME_ASSET, connectorSpecRequest.getPath());
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
