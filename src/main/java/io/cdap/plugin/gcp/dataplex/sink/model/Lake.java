package io.cdap.plugin.gcp.dataplex.sink.model;

import com.google.gson.annotations.SerializedName;

/**
 * Holds Lake details
 */
public class Lake {
  public String name;
  public String displayName;
  public String uid;
  public String createTime;
  public String updateTime;
  public String state;
  public String serviceAccount;
  @SerializedName("securitySpec")
  public LakeSecuritySpec lakeSecuritySpec;
  @SerializedName("securityStatus")
  public LakeSecurityStatus lakeSecurityStatus;
  @SerializedName("metastore")
  public LakeMetastore lakeMetastore;
  @SerializedName("assetStatus")
  public AssetLakeStatus assetLakeStatus;

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

  public String getServiceAccount() {
    return serviceAccount;
  }

  public void setServiceAccount(String serviceAccount) {
    this.serviceAccount = serviceAccount;
  }

  public LakeSecuritySpec getLakeSecuritySpec() {
    return lakeSecuritySpec;
  }

  public void setLakeSecuritySpec(LakeSecuritySpec lakeSecuritySpec) {
    this.lakeSecuritySpec = lakeSecuritySpec;
  }

  public LakeSecurityStatus getLakeSecurityStatus() {
    return lakeSecurityStatus;
  }

  public void setLakeSecurityStatus(LakeSecurityStatus lakeSecurityStatus) {
    this.lakeSecurityStatus = lakeSecurityStatus;
  }

  public LakeMetastore getLakeMetastore() {
    return lakeMetastore;
  }

  public void setLakeMetastore(LakeMetastore lakeMetastore) {
    this.lakeMetastore = lakeMetastore;
  }

  public AssetLakeStatus getAssetLakeStatus() {
    return assetLakeStatus;
  }

  public void setAssetLakeStatus(AssetLakeStatus assetLakeStatus) {
    this.assetLakeStatus = assetLakeStatus;
  }
}

class LakeSecuritySpec {
}

class LakeSecurityStatus {
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

class LakeMetastore {
}

class AssetLakeStatus {
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
