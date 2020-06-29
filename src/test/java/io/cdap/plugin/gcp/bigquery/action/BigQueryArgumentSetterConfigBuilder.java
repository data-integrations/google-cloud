package io.cdap.plugin.gcp.bigquery.action;

public final class BigQueryArgumentSetterConfigBuilder {

  private String referenceName;
  private String dataset;
  private String table;
  private String argumentSelectionConditions;
  private String argumentsColumns;

  private BigQueryArgumentSetterConfigBuilder() {}

  public static BigQueryArgumentSetterConfigBuilder bigQueryArgumentSetterConfig() {
    return new BigQueryArgumentSetterConfigBuilder();
  }

  public BigQueryArgumentSetterConfigBuilder setReferenceName(String referenceName) {
    this.referenceName = referenceName;
    return this;
  }

  public BigQueryArgumentSetterConfigBuilder setDataset(String dataset) {
    this.dataset = dataset;
    return this;
  }

  public BigQueryArgumentSetterConfigBuilder setTable(String table) {
    this.table = table;
    return this;
  }

  public BigQueryArgumentSetterConfigBuilder setArgumentSelectionConditions(
      String argumentSelectionConditions) {
    this.argumentSelectionConditions = argumentSelectionConditions;
    return this;
  }

  public BigQueryArgumentSetterConfigBuilder setArgumentsColumns(String argumentsColumns) {
    this.argumentsColumns = argumentsColumns;
    return this;
  }

  public BigQueryArgumentSetterConfig build() {
    return new BigQueryArgumentSetterConfig(
        referenceName, dataset, table, argumentSelectionConditions, argumentsColumns);
  }
}
