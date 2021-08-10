package io.cdap.plugin.gcp.dataplex.sink.model;

import java.util.List;

/**
 * Holds Locations, Zones, Assets and lakes as list
 */
public class ModelWrapper {
  List<Location> locations;
  List<Asset> assets;
  List<Zone> zones;
  List<Lake> lakes;

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
