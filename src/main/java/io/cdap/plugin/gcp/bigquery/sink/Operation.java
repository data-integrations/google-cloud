package io.cdap.plugin.gcp.bigquery.sink;

/**
 * The type of write operation.
 */
public enum Operation {
  INSERT,
  UPDATE,
  UPSERT
}
