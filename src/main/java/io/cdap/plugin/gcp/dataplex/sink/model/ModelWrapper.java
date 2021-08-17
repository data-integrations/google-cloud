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

package io.cdap.plugin.gcp.dataplex.sink.model;

import java.util.ArrayList;
import java.util.List;

/**
 * Holds Locations, Zones, Assets and lakes as list
 */
public class ModelWrapper {
  List<Location> locations = new ArrayList<>();
  List<Asset> assets  = new ArrayList<>();
  List<Zone> zones  = new ArrayList<>();
  List<Lake> lakes  = new ArrayList<>();

  public List<Location> getLocations() {
    return locations;
  }

  public void setLocations(List<Location> locations) {
    this.locations = locations;
  }

  public List<Asset> getAssets() {
    return assets;
  }

  public void setAssets(List<Asset> assets) {
    this.assets = assets;
  }

  public List<Zone> getZones() {
    return zones;
  }

  public void setZones(List<Zone> zones) {
    this.zones = zones;
  }

  public List<Lake> getLakes() {
    return lakes;
  }

  public void setLakes(List<Lake> lakes) {
    this.lakes = lakes;
  }
}
