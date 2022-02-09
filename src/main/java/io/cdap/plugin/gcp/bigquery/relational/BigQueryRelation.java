package io.cdap.plugin.gcp.bigquery.relational;

import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
import io.cdap.cdap.etl.api.aggregation.GroupByAggregationDefinition;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.Relation;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * An implementation of {@link Relation} designed to operate on BigQuery.
 */
public class BigQueryRelation implements Relation {
  private final boolean isValid;
  private final String validationError;

  private final Map<String, Expression> columns;
  private final Expression filter;
  private final Relation baseRelation;
  private final String datasetName;

  /**
   * Initializes a new {@link BigQueryRelation} using the set of columns specified.
   * @param columnSet set of strings indicating column names in the relation.
   */
  public BigQueryRelation(String datasetName, Set<String> columnSet) {
    SQLExpressionFactory factory = new SQLExpressionFactory();

    Map<String, Expression> columns = new HashMap<>();
    for (String column: columnSet) {
      columns.put(column, factory.compile(column));
    }
    this.datasetName = datasetName;
    this.columns = columns;
    this.isValid = true;
    this.validationError = null;
    this.filter = null;
    this.baseRelation = null;
  }

  private BigQueryRelation(String datasetName,
                           Map<String, Expression> columns,
                           Expression filter,
                           Relation baseRelation) {
    this(datasetName, columns, filter, baseRelation, true, null);
  }

  private BigQueryRelation(String datasetName,
                           Map<String, Expression> columns,
                           Expression filter,
                           Relation baseRelation,
                           boolean isValid,
                           String validationError) {
    this.datasetName = datasetName;
    this.columns = columns;
    this.filter = filter;
    this.baseRelation = baseRelation;
    this.isValid = isValid;
    this.validationError = validationError;
  }

  private Relation getInvalidRelation(String validationError) {
    return new BigQueryRelation(null, null, null, null, false, validationError);
  }

  @Override
  public boolean isValid() {
    return isValid;
  }

  @Override
  public String getValidationError() {
    return isValid() ? null : validationError;
  }

  public Relation getBaseRelation() {
    return baseRelation;
  }

  @Override
  public Relation setColumn(String column, Expression value) {
    Map<String, Expression> newColumns = new HashMap<>(columns);
    newColumns.put(column, value);

    return new BigQueryRelation(datasetName, newColumns, filter, this);
  }

  @Override
  public Relation dropColumn(String column) {
    Map<String, Expression> newColumns = new HashMap<>(columns);
    newColumns.remove(column);

    return new BigQueryRelation(datasetName, newColumns, filter, this);
  }

  @Override
  public Relation select(Map<String, Expression> columns) {
    return new BigQueryRelation(datasetName, columns, filter, this);
  }

  @Override
  public Relation filter(Expression filter) {
    return new BigQueryRelation(datasetName, columns, filter, this);
  }

  @Override
  public Relation groupBy(GroupByAggregationDefinition aggregationDefinition) {
    return getInvalidRelation("Group by operation not supported currently.");
  }

  @Override
  public Relation deduplicate(DeduplicateAggregationDefinition aggregationDefinition) {
    return getInvalidRelation("Deduplicate operation not supported currently.");
  }
}
