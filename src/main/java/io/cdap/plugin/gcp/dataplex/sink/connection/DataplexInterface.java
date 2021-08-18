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

package io.cdap.plugin.gcp.dataplex.sink.connection;

import com.google.auth.oauth2.GoogleCredentials;
import io.cdap.plugin.gcp.dataplex.sink.exception.ConnectorException;
import io.cdap.plugin.gcp.dataplex.sink.model.Asset;
import io.cdap.plugin.gcp.dataplex.sink.model.Lake;
import io.cdap.plugin.gcp.dataplex.sink.model.Location;
import io.cdap.plugin.gcp.dataplex.sink.model.Zone;

import java.util.List;

/**
 * Exposes the APIs to connect with and execute programs in Dataplex
 */
public interface DataplexInterface {

    List<Location> listLocations(GoogleCredentials credentials, String projectId) throws ConnectorException;

    Location getLocation(GoogleCredentials credentials, String projectId, String location) throws ConnectorException;

    List<Lake> listLakes(GoogleCredentials credentials, String projectId,
                         String location) throws ConnectorException;

    Lake getLake(GoogleCredentials credentials, String projectId, String location, String lakeId)
      throws ConnectorException;

    List<Zone> listZones(GoogleCredentials credentials, String projectId,
                         String location, String lakeId) throws ConnectorException;

    Zone getZone(GoogleCredentials credentials, String projectId, String location, String lakeId, String zoneId)
      throws ConnectorException;

    List<Asset> listAssets(GoogleCredentials credentials, String projectId,
                           String location, String lakeId, String zoneId) throws ConnectorException;

    Asset getAsset(GoogleCredentials credentials, String projectId,
                   String location, String lakeId, String zoneId, String assetId) throws ConnectorException;
}
