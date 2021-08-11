package io.cdap.plugin.gcp.dataplex.sink.config;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Contains Dataplex config properties.
 */
public class DataplexBaseConfig extends PluginConfig {
    public static final String REFERENCE_NAME = "referenceName";
    public static final String NAME_LAKE = "lake";
    public static final String NAME_ZONE = "zone";
    public static final String NAME_ASSET = "asset";
    public static final String NAME_ASSET_TYPE = "assetType";
    public static final String NAME_LOCATION = "location";



    @Name(REFERENCE_NAME)
    @Description("Name used to uniquely identify this sink for lineage, annotating metadata, etc.")
    protected String referenceName;

    @Name(NAME_LOCATION)
    @Macro
    @Description("Enter location where lake got created. Example us-central1")
    protected String location;

    @Name(NAME_LAKE)
    @Description("Lake name to use. User can type it in or " +
      "press a browse button which enables hierarchical selection.")
    @Macro
    protected String lake;

    @Name(NAME_ZONE)
    @Description("Zone name to use. User can type it in or " +
      "press a browse button which enables hierarchical selection.")
    @Macro
    protected String zone;

    @Name(NAME_ASSET)
    @Description("Asset name to use. User can type it in or " +
            "press a browse button which enables hierarchical selection.")
    @Macro
    protected String asset;


    @Nullable
    @Name(NAME_ASSET_TYPE)
    @Description("Asset Type can be BQ Dataset or GCS bucket.")
    protected String assetType;


    public String getReferenceName() {
        return referenceName;
    }

    public String getAsset() {
        return asset;
    }

    public String getAssetType() {
        return assetType;
    }

    public String getLake() {
        return lake;
    }

    public void setLake(String lake) {
        this.lake = lake;
    }

    public String getZone() {
        return zone;
    }

    public void setZone(String zone) {
        this.zone = zone;
    }

    public String getLocation() {
        return location;
    }
}
