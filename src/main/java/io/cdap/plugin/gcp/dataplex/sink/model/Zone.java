package io.cdap.plugin.gcp.dataplex.sink.model;

import com.google.gson.annotations.SerializedName;

/**
 * Holds Zone details
 */
public class Zone {
  public String name;
  public String displayName;
  public String uid;
  public String createTime;
  public String updateTime;
  public String state;
  public String type;
  @SerializedName("securitySpec")
  public ZoneSecuritySpec zoneSecuritySpec;
  @SerializedName("securityStatus")
  public ZoneSecurityStatus zoneSecurityStatus;
  @SerializedName("discoverySpec")
  public ZoneDiscoverySpec zoneDiscoverySpec;
  @SerializedName("resourceSpec")
  public ZoneResourceSpec zoneResourceSpec;
  @SerializedName("assetStatus")
  public ZoneAssetStatus zoneAssetStatus;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public String getUid() {
    return uid;
  }

  public void setUid(String uid) {
    this.uid = uid;
  }

  public String getCreateTime() {
    return createTime;
  }

  public void setCreateTime(String createTime) {
    this.createTime = createTime;
  }

  public String getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public ZoneSecuritySpec getZoneSecuritySpec() {
    return zoneSecuritySpec;
  }

  public void setZoneSecuritySpec(ZoneSecuritySpec zoneSecuritySpec) {
    this.zoneSecuritySpec = zoneSecuritySpec;
  }

  public ZoneSecurityStatus getZoneSecurityStatus() {
    return zoneSecurityStatus;
  }

  public void setZoneSecurityStatus(ZoneSecurityStatus zoneSecurityStatus) {
    this.zoneSecurityStatus = zoneSecurityStatus;
  }

  public ZoneDiscoverySpec getZoneDiscoverySpec() {
    return zoneDiscoverySpec;
  }

  public void setZoneDiscoverySpec(ZoneDiscoverySpec zoneDiscoverySpec) {
    this.zoneDiscoverySpec = zoneDiscoverySpec;
  }

  public ZoneResourceSpec getZoneResourceSpec() {
    return zoneResourceSpec;
  }

  public void setZoneResourceSpec(ZoneResourceSpec zoneResourceSpec) {
    this.zoneResourceSpec = zoneResourceSpec;
  }

  public ZoneAssetStatus getZoneAssetStatus() {
    return zoneAssetStatus;
  }

  public void setZoneAssetStatus(ZoneAssetStatus zoneAssetStatus) {
    this.zoneAssetStatus = zoneAssetStatus;
  }
}

class ZoneSecuritySpec {
}

class ZoneSecurityStatus {
  public String state;
  public String updateTime;

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
  }
}

class ZoneMetastore {
  public boolean enabled;
  public String databaseName;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }
}

class ZoneBigquery {
  public boolean enabled;
  public String datasetName;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getDatasetName() {
    return datasetName;
  }

  public void setDatasetName(String datasetName) {
    this.datasetName = datasetName;
  }
}

class ZonePublishing {
  @SerializedName("metastore")
  public ZoneMetastore zoneMetastore;
  @SerializedName("bigquery")
  public ZoneBigquery zoneBigquery;

  public ZoneMetastore getZoneMetastore() {
    return zoneMetastore;
  }

  public void setZoneMetastore(ZoneMetastore zoneMetastore) {
    this.zoneMetastore = zoneMetastore;
  }

  public ZoneBigquery getZoneBigquery() {
    return zoneBigquery;
  }

  public void setZoneBigquery(ZoneBigquery zoneBigquery) {
    this.zoneBigquery = zoneBigquery;
  }
}

class ZoneDiscoverySpec {
  public boolean enabled;
  public String schedule;
  @SerializedName("publishing")
  public ZonePublishing zonePublishing;

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getSchedule() {
    return schedule;
  }

  public void setSchedule(String schedule) {
    this.schedule = schedule;
  }

  public ZonePublishing getZonePublishing() {
    return zonePublishing;
  }

  public void setZonePublishing(ZonePublishing zonePublishing) {
    this.zonePublishing = zonePublishing;
  }
}

class ZoneResourceSpec {
  public String locationType;

  public String getLocationType() {
    return locationType;
  }

  public void setLocationType(String locationType) {
    this.locationType = locationType;
  }
}

class ZoneAssetStatus {
  public String updateTime;
  public int activeAssets;

  public String getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
  }

  public int getActiveAssets() {
    return activeAssets;
  }

  public void setActiveAssets(int activeAssets) {
    this.activeAssets = activeAssets;
  }
}
