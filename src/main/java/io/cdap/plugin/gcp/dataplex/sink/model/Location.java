package io.cdap.plugin.gcp.dataplex.sink.model;

/**
 * Holds Location details
 */
public class Location {
  public String name;
  public String locationId;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getLocationId() {
    return locationId;
  }

  public void setLocationId(String locationId) {
    this.locationId = locationId;
  }
}
