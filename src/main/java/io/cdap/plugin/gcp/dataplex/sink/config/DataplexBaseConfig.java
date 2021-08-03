package io.cdap.plugin.gcp.dataplex.sink.config;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;

/**
 * Contains Dataplex config properties.
 */
public class DataplexBaseConfig extends PluginConfig {
    public static final String REFERENCE_NAME = "referenceName";
    public static final String NAME_ASSET = "asset";
    public static final String NAME_ASSET_TYPE = "assetType";



    @Name(REFERENCE_NAME)
    @Description("Name used to uniquely identify this sink for lineage, annotating metadata, etc.")
    protected String referenceName;

    @Name(NAME_ASSET)
    @Description("Asset name to use. User can type it in (format Lake>Zone>Asset) or " +
            "press a browse button which enables hierarchical selection.")
    @Macro
    protected String asset;


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



}
