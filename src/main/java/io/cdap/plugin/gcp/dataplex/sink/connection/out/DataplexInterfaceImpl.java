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

package io.cdap.plugin.gcp.dataplex.sink.connection.out;

import com.google.gson.Gson;
import io.cdap.plugin.gcp.dataplex.sink.connection.DataplexInterface;
import io.cdap.plugin.gcp.dataplex.sink.model.Asset;
import io.cdap.plugin.gcp.dataplex.sink.model.Lake;
import io.cdap.plugin.gcp.dataplex.sink.model.Location;
import io.cdap.plugin.gcp.dataplex.sink.model.ModelWrapper;
import io.cdap.plugin.gcp.dataplex.sink.model.Zone;
import io.cdap.plugin.gcp.dataplex.sink.util.DataplexApiHelper;

import java.text.MessageFormat;
import java.util.List;
import java.util.logging.Logger;

/**
 * Implementation of APIs to connect with and execute operations in Dataplex
 */
public class DataplexInterfaceImpl implements DataplexInterface {

    public static final String HOST = "https://dataplex.googleapis.com";
    static final Logger LOGGER = Logger.getLogger(DataplexInterfaceImpl.class.getName());

    @Override
    public List<Location> listLocations(String projectId) {
        LOGGER.info(MessageFormat.format("Invoked to fetch the list of locations from project id ", projectId));
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST)
          .append("/v1/projects/")
          .append(projectId)
          .append("/locations");
        ModelWrapper locations =
          gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), "filePath"), ModelWrapper.class);
        return locations.getLocations();
    }

    @Override
    public Location getLocation(String projectId, String locationId) {
        LOGGER.info(MessageFormat.format("gets location based on location id ", locationId));
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST)
          .append("/v1/projects/")
          .append(projectId)
          .append("/locations/")
          .append(locationId);
        Location location = gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(),
          "filePath"), Location.class);
        return location;
    }

    @Override
    public List<Lake> listLakes(String projectId, String location) {
        LOGGER.info(MessageFormat.format("fetches the list of lakes from location ", location));
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST)
          .append("/v1/projects/")
          .append(projectId)
          .append("/locations/")
          .append(location)
          .append("/lakes");
        ModelWrapper lakes =
          gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), "filePath"), ModelWrapper.class);
        return lakes.getLakes();
    }

    @Override
    public Lake getLake(String projectId, String location, String lakeId) {
        LOGGER.info(MessageFormat.format("gets the lake based on lake id ", lakeId));
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST)
          .append("/v1/projects/")
          .append(projectId)
          .append("/locations/")
          .append(location)
          .append("/lakes/")
          .append(lakeId);
        Lake lake = gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), "filePath"), Lake.class);
        return lake;
    }

    @Override
    public List<Zone> listZones(String projectId, String location, String lakeId) {
        LOGGER.info(MessageFormat.format("fetches the list of zones by lake id ", lakeId));
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST)
          .append("/v1/projects/")
          .append(projectId)
          .append("/locations/")
          .append(location)
          .append("/lakes/")
          .append(lakeId)
          .append("/zones");
        ModelWrapper zones =
          gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), "filePath"), ModelWrapper.class);
        return zones.getZones();
    }

    @Override
    public Zone getZone(String projectId, String location, String lakeId, String zoneId) {
        LOGGER.info(MessageFormat.format("gets the details of zone based on zone id ", zoneId));
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST)
          .append("/v1/projects/")
          .append(projectId)
          .append("/locations/")
          .append(location)
          .append("/lakes/")
          .append(lakeId)
          .append("/zones/")
          .append(zoneId);
        Zone zone = gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), "filePath"), Zone.class);
        return zone;
    }

    @Override
    public List<Asset> listAssets(String projectId, String location, String lakeId, String zoneId) {
        LOGGER.info(MessageFormat.format("fetches the list of assets based on zone Id", zoneId));
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST)
          .append("/v1/projects/")
          .append(projectId)
          .append("/locations/")
          .append(location)
          .append("/lakes/")
          .append(lakeId)
          .append("/zones/")
          .append(zoneId)
          .append("/assets/");
        ModelWrapper assets =
          gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), "filePath"), ModelWrapper.class);
        return assets.getAssets();
    }

    @Override
    public Asset getAsset(String projectId, String location, String lakeId, String zoneId, String assetId) {
        LOGGER.info(MessageFormat.format("gets the details of asset based on asset Id", assetId));
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST)
          .append("/v1/projects/")
          .append(projectId)
          .append("/locations/")
          .append(location)
          .append("/lakes/")
          .append(lakeId)
          .append("/zones/")
          .append(zoneId)
          .append("/assets/")
          .append(assetId);
        Asset asset = gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(),
          "filePath"), Asset.class);
        return asset;
    }
}
