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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.Gson;
import io.cdap.plugin.gcp.dataplex.sink.connection.DataplexInterface;
import io.cdap.plugin.gcp.dataplex.sink.exception.ConnectorException;
import io.cdap.plugin.gcp.dataplex.sink.model.Asset;
import io.cdap.plugin.gcp.dataplex.sink.model.Lake;
import io.cdap.plugin.gcp.dataplex.sink.model.Location;
import io.cdap.plugin.gcp.dataplex.sink.model.ModelWrapper;
import io.cdap.plugin.gcp.dataplex.sink.model.Zone;
import io.cdap.plugin.gcp.dataplex.sink.util.DataplexApiHelper;

import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Implementation of APIs to connect with and execute operations in Dataplex
 */
public class DataplexInterfaceImpl implements DataplexInterface {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(DataplexInterfaceImpl.class);
    public static final String HOST = "https://dataplex.googleapis.com";
    public static final String PROJECT = "/v1/projects/";
    public static final String LOCATION = "/locations/";
    public static final String LAKES = "/lakes/";
    public static final String ZONES = "/zones/";
    public static final String ASSETS = "/assets/";

    @Override
    public List<Location> listLocations(GoogleCredentials credentials,
                                        String projectId) throws ConnectorException {
        LOGGER.info("Invoking to fetch the list of Dataplex locations for project id '{}'", projectId);
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST).append(PROJECT).append(projectId).append(LOCATION);
        ModelWrapper locations =
          gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), credentials), ModelWrapper.class);
        return locations.getLocations();
    }

    @Override
    public Location getLocation(GoogleCredentials credentials, String projectId, String locationId)
      throws ConnectorException {
        LOGGER.info("Retrieves Dataplex location based on location id '{}'", locationId);
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST).append(PROJECT).append(projectId).append(LOCATION).append(locationId);
        return gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), credentials), Location.class);
    }

    @Override
    public List<Lake> listLakes(GoogleCredentials credentials, String projectId,
                                String location) throws ConnectorException {
        LOGGER.info("Retrieves the list of Dataplex lakes from location '{}'", location);
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST).append(PROJECT).append(projectId).append(LOCATION).append(location).append(LAKES);
        ModelWrapper lakes =
          gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), credentials), ModelWrapper.class);
        return lakes.getLakes();
    }

    @Override
    public Lake getLake(GoogleCredentials credentials, String projectId, String location, String lakeId)
      throws ConnectorException {
        LOGGER.info("Retrieves the Dataplex lake based on lake id '{}'", lakeId);
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST).append(PROJECT).append(projectId).append(LOCATION).append(location).append(LAKES)
          .append(lakeId);
        return gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), credentials), Lake.class);
    }

    @Override
    public List<Zone> listZones(GoogleCredentials credentials, String projectId,
                                String location, String lakeId) throws ConnectorException {
        LOGGER.info("Retrieves the list of Dataplex zones by lake id '{}'", lakeId);
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST).append(PROJECT).append(projectId).append(LOCATION).append(location).append(LAKES)
          .append(lakeId).append(ZONES);
        ModelWrapper zones =
          gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), credentials), ModelWrapper.class);
        return zones.getZones();
    }

    @Override
    public Zone getZone(GoogleCredentials credentials, String projectId, String location, String lakeId,
                        String zoneId) throws ConnectorException {
        LOGGER.info("Retrieves the details of Dataplex zone based on zone id '{}'", zoneId);
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST).append(PROJECT).append(projectId).append(LOCATION).append(location).append(LAKES)
          .append(lakeId).append(ZONES).append(zoneId);
        return gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), credentials), Zone.class);
    }

    @Override
    public List<Asset> listAssets(GoogleCredentials credentials, String projectId,
                                  String location, String lakeId, String zoneId) throws ConnectorException {
        LOGGER.info("Retrieves the list of Dataplex assets based on zone Id '{}'", zoneId);
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST).append(PROJECT).append(projectId).append(LOCATION).append(location)
          .append(LAKES).append(lakeId).append(ZONES).append(zoneId).append(ASSETS);
        ModelWrapper assets =
          gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), credentials), ModelWrapper.class);
        return assets.getAssets();
    }

    @Override
    public Asset getAsset(GoogleCredentials credentials, String projectId,
                          String location, String lakeId, String zoneId, String assetId) throws ConnectorException {
        LOGGER.info("Retrieves the details of Dataplex asset based on asset Id '{}'", assetId);
        StringBuilder urlBuilder = new StringBuilder();
        Gson gson = new Gson();
        DataplexApiHelper dataplexApiHelper = new DataplexApiHelper();
        urlBuilder.append(HOST).append(PROJECT).append(projectId).append(LOCATION).append(location).append(LAKES)
          .append(lakeId).append(ZONES).append(zoneId).append(ASSETS).append(assetId);
        return gson.fromJson(dataplexApiHelper.invokeDataplexApi(urlBuilder.toString(), credentials), Asset.class);
    }
}
