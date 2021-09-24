package io.cdap.plugin.gcp.bigquery.sqlengine.dl;

import io.cdap.cdap.etl.api.dl.DLDataSet;
import io.cdap.cdap.etl.api.dl.DLExpression;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLDataset;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A DL DataSet implementation that allows to build chain of transformations that later can be executed in BQ.
 */
public class BigQueryDLDataSet implements DLDataSet {
  private final SQLDataset sourceDataSet;
  //TODO: Think about case sensitivity
  private final Set<String> columns;
  private final boolean transformNeeded;
  private final String transformExpression;

  public BigQueryDLDataSet(String project,
                           String bqDataset,
                           Map<String, String> stageToBQTableNameMap,
                           SQLDataset sourceDataSet) {
    this.sourceDataSet = sourceDataSet;
    this.columns = sourceDataSet.getSchema().getFields().stream()
        .map(f -> f.getName()).collect(Collectors.toCollection(LinkedHashSet::new));
    this.transformNeeded = false;
    //TODO: use proper name
    this.transformExpression = String.format("select * from `%s.%s.%s`",
                                             project, bqDataset,
                                             stageToBQTableNameMap.get(sourceDataSet.getDatasetName()));
  }

  private BigQueryDLDataSet(SQLDataset sourceDataSet, Set<String> columns,
      String transformExpression) {
    this.sourceDataSet = sourceDataSet;
    this.columns = columns;
    this.transformNeeded = true;
    this.transformExpression = transformExpression;
  }

  @Override
  public DLDataSet setColumn(String column, DLExpression value) {
    Set<String> newColumns = new LinkedHashSet<>(columns);
    newColumns.remove(column);
    String selectList = Stream.concat(
        newColumns.stream(),
        Stream.of("(" + value + ") as " + column)
    ).collect(Collectors.joining(","));
    String transformExpression = "select "
        + selectList
        + getFromExpression();
    newColumns.add(column);
    return new BigQueryDLDataSet(sourceDataSet, newColumns, transformExpression);
  }

  @Override
  public DLDataSet dropColumn(String column) {
    Set<String> newColumns = new LinkedHashSet<>(columns);
    newColumns.remove(column);
    String selectList = String.join(",", newColumns);
    String transformExpression = "select "
        + selectList
        + getFromExpression();
    newColumns.add(column);
    return new BigQueryDLDataSet(sourceDataSet, newColumns, transformExpression);
  }

  @Override
  public DLDataSet select(Map<String, DLExpression> columns) {
    String selectList =  columns.entrySet().stream()
        .map(e -> "(" + e.getValue() + ") as " + e.getKey())
        .collect(Collectors.joining(","));
    String transformExpression = "select "
        + selectList + getFromExpression();
    return new BigQueryDLDataSet(
        sourceDataSet,
        new LinkedHashSet<>(columns.keySet()),
        transformExpression);
  }

  @Override
  public DLDataSet filter(DLExpression filter) {
    String transformExpression = "select * " + getFromExpression() + " where " + filter;
    return new BigQueryDLDataSet(
        sourceDataSet,
        columns,
        transformExpression
    );
  }

  public SQLDataset getSourceDataSet() {
    return sourceDataSet;
  }

  public boolean isTransformNeeded() {
    return transformNeeded;
  }

  public String getTransformExpression() {
    return transformExpression;
  }

  private String getFromExpression() {
    //TODO: alias validation
    return " from (" + this.transformExpression + ") as " + sourceDataSet.getDatasetName();
  }
}
