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

import io.cdap.plugin.gcp.dataplex.sink.model.Asset;
import io.cdap.plugin.gcp.dataplex.sink.model.Lake;
import io.cdap.plugin.gcp.dataplex.sink.model.Location;
import io.cdap.plugin.gcp.dataplex.sink.model.Zone;

import java.util.List;

/**
 * Exposes the APIs to connect with and execute programs in Dataplex
 */
public interface DataplexInterface {

    List<Location> listLocations(String projectId);

    Location getLocation(String projectId, String location);

    List<Lake> listLakes(String projectId, String location);

    Lake getLake(String projectId, String location, String lakeId);

    List<Zone> listZones(String projectId, String location, String lakeId);

    Zone getZone(String projectId, String location, String lakeId, String zoneId);

    List<Asset> listAssets(String projectId, String location, String lakeId, String zoneId);

    Asset getAsset(String projectId, String location, String lakeId, String zoneId, String assetId);
}
