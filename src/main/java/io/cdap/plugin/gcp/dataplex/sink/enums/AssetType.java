package io.cdap.plugin.gcp.dataplex.sink.enums;

/**
 * Dataplex asset types
 */
public enum AssetType {

  STORAGE_BUCKET("Storage Bucket"),
  BIGQUERY_DATASET("BigQuery Dataset");

  private final String assetType;

  AssetType(String assetType) {
    this.assetType = assetType;
  }

  public String getAssetType() {
    return assetType;
  }
}
