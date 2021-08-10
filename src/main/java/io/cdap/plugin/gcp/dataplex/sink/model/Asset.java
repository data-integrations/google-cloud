package io.cdap.plugin.gcp.dataplex.sink.model;

import com.google.gson.annotations.SerializedName;

/**
 * Information about Asset
 */
public class Asset {
  public String name;
  public String displayName;
  public String uid;
  public String createTime;
  public String updateTime;
  public String state;
  @SerializedName("resourceSpec")
  public AssetResourceSpec assetResourceSpec;
  @SerializedName("resourceStatus")
  public AssetResourceStatus assetResourceStatus;
  @SerializedName("SecuritySpec")
  public AssetSecuritySpec assetSecuritySpec;
  @SerializedName("securityStatus")
  public AssetSecurityStatus assetSecurityStatus;
  @SerializedName("discoverySpec")
  public AssetDiscoverySpec assetDiscoverySpec;
  @SerializedName("discoveryStatus")
  public AssetDiscoveryStatus assetDiscoveryStatus;

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

  public AssetResourceSpec getAssetResourceSpec() {
    return assetResourceSpec;
  }

  public void setAssetResourceSpec(AssetResourceSpec assetResourceSpec) {
    this.assetResourceSpec = assetResourceSpec;
  }

  public AssetResourceStatus getAssetResourceStatus() {
    return assetResourceStatus;
  }

  public void setAssetResourceStatus(AssetResourceStatus assetResourceStatus) {
    this.assetResourceStatus = assetResourceStatus;
  }

  public AssetSecuritySpec getAssetSecuritySpec() {
    return assetSecuritySpec;
  }

  public void setAssetSecuritySpec(AssetSecuritySpec assetSecuritySpec) {
    this.assetSecuritySpec = assetSecuritySpec;
  }

  public AssetSecurityStatus getAssetSecurityStatus() {
    return assetSecurityStatus;
  }

  public void setAssetSecurityStatus(AssetSecurityStatus assetSecurityStatus) {
    this.assetSecurityStatus = assetSecurityStatus;
  }

  public AssetDiscoverySpec getAssetDiscoverySpec() {
    return assetDiscoverySpec;
  }

  public void setAssetDiscoverySpec(AssetDiscoverySpec assetDiscoverySpec) {
    this.assetDiscoverySpec = assetDiscoverySpec;
  }

  public AssetDiscoveryStatus getAssetDiscoveryStatus() {
    return assetDiscoveryStatus;
  }

  public void setAssetDiscoveryStatus(AssetDiscoveryStatus assetDiscoveryStatus) {
    this.assetDiscoveryStatus = assetDiscoveryStatus;
  }
}

class AssetResourceSpec {
  public String name;
  public String type;
  public String creationPolicy;
  public String deletionPolicy;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getCreationPolicy() {
    return creationPolicy;
  }

  public void setCreationPolicy(String creationPolicy) {
    this.creationPolicy = creationPolicy;
  }

  public String getDeletionPolicy() {
    return deletionPolicy;
  }

  public void setDeletionPolicy(String deletionPolicy) {
    this.deletionPolicy = deletionPolicy;
  }
}

class AssetResourceStatus {
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

class AssetSecuritySpec {
}

class AssetSecurityStatus {
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

class AssetDiscoverySpec {
  public boolean enabled;
  public String schedule;

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
}

class AssetStats {
}

class AssetDiscoveryStatus {
  public String state;
  public String updateTime;
  public String lastRunTime;
  public String nextRunTime;
  @SerializedName("stats")
  public AssetStats assetStats;
  public String lastRunDuration;

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

  public String getLastRunTime() {
    return lastRunTime;
  }

  public void setLastRunTime(String lastRunTime) {
    this.lastRunTime = lastRunTime;
  }

  public String getNextRunTime() {
    return nextRunTime;
  }

  public void setNextRunTime(String nextRunTime) {
    this.nextRunTime = nextRunTime;
  }

  public AssetStats getAssetStats() {
    return assetStats;
  }

  public void setAssetStats(AssetStats assetStats) {
    this.assetStats = assetStats;
  }

  public String getLastRunDuration() {
    return lastRunDuration;
  }

  public void setLastRunDuration(String lastRunDuration) {
    this.lastRunDuration = lastRunDuration;
  }
}
