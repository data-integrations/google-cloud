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
import io.cdap.plugin.gcp.dataplex.sink.config.DataplexBatchSinkConfig;
import io.cdap.plugin.gcp.dataplex.sink.enums.AssetType;

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

    @Override
    public void test(ConnectorContext connectorContext) throws ValidationException {

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
            return listZones(10);
        }
        String asset = path.getAsset();
        if (asset == null) {
            return listAssets(10);
        }
        BrowseDetail.Builder builder = BrowseDetail.builder();
        builder.addEntity(BrowseEntity.builder(asset, asset, "ASSET").canBrowse(false).canSample(true).build());
        return builder.setTotalCount(1).build();
    }

    private BrowseDetail listLakes(Integer limit) {
        BrowseDetail.Builder builder = BrowseDetail.builder();
        String name = "lakes";
        builder.addEntity(BrowseEntity.builder(name, name, "LAKE").canBrowse(true).canSample(true).build());
        return builder.setTotalCount(1).build();
    }

    private BrowseDetail listZones(Integer limit) {
        BrowseDetail.Builder builder = BrowseDetail.builder();
        String name = "zones";
        builder.addEntity(BrowseEntity.builder(name, name, "ZONE").canBrowse(true).canSample(true).build());
        return builder.setTotalCount(1).build();
    }

    private BrowseDetail listAssets(Integer limit) {
        BrowseDetail.Builder builder = BrowseDetail.builder();
        String name = "assets";
        builder.addEntity(BrowseEntity.builder(name, name, "ASSET").canBrowse(false).canSample(true).build());
        return builder.setTotalCount(1).build();
    }

    @Override
    public ConnectorSpec generateSpec(ConnectorContext connectorContext, ConnectorSpecRequest connectorSpecRequest)
      throws IOException {
        ConnectorSpec.Builder specBuilder = ConnectorSpec.builder();
        Map<String, String> properties = new HashMap<>();
        properties.put(DataplexBaseConfig.NAME_ASSET, connectorSpecRequest.getPath());
        properties.put(DataplexBaseConfig.NAME_ASSET_TYPE, AssetType.STORAGE_BUCKET.name());
        return specBuilder.addRelatedPlugin(new PluginSpec(DataplexBatchSink.NAME, BatchSink.PLUGIN_TYPE, properties))
          .build();
    }

    @Override
    public List<StructuredRecord> sample(ConnectorContext connectorContext, SampleRequest sampleRequest)
      throws IOException {
        return Collections.emptyList();
    }
}
