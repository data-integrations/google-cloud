package io.cdap.plugin.gcp.bigquery.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;

/**
 *
 */
public class BigQueryConnection {

  @Name(BigQuerySourceConfig.NAME_DATASET)
  @Macro
  @Description("The dataset the table belongs to. A dataset is contained within a specific project. "
                 + "Datasets are top-level containers that are used to organize and contro" +
                 "l access to tables and views.")
  private String dataset;

  @Name(BigQuerySourceConfig.NAME_TABLE)
  @Macro
  @Description("The table to read from. A table contains individual records organized in rows. "
                 + "Each record is composed of columns (also called fields). "
                 + "Every table is defined by a schema that describes the column names, data " +
                 "types, and other information.")
  private String table;

  public String getTable() {
    return table;
  }

  public String getDataset() {
    return dataset;
  }
}
