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

package io.cdap.plugin.gcp.dataplex.common.config;

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
    @Description("Resource id for the Dataplex location. User can type it in or press a browse button which enables" +
      " hierarchical selection.")
    protected String location;

    @Name(NAME_LAKE)
    @Macro
    @Description("Resource id for the Dataplex lake. User can type it in or press a browse button which enables " +
      "hierarchical selection.")
    protected String lake;

    @Name(NAME_ZONE)
    @Macro
    @Description("Resource id for the Dataplex zone. User can type it in or press a browse button which enables " +
      "hierarchical selection.")
    protected String zone;

    @Name(NAME_ASSET)
    @Macro
    @Description("Resource id for the Dataplex asset. It represents a cloud resource that is being managed within a" +
      " lake as a member of a zone. User can type it in or press a browse button which enables " +
      "hierarchical selection.")
    protected String asset;

    @Name(NAME_ASSET_TYPE)
    @Nullable
    @Description("Asset type resource.")
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
