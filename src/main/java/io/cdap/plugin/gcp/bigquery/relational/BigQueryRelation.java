package io.cdap.plugin.gcp.bigquery.relational;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
import io.cdap.cdap.etl.api.aggregation.GroupByAggregationDefinition;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.plugin.gcp.bigquery.sqlengine.BigQuerySQLDataset;
import io.cdap.plugin.gcp.bigquery.sqlengine.builder.BigQueryDeduplicateSQLBuilder;
import io.cdap.plugin.gcp.bigquery.sqlengine.builder.BigQueryGroupBySQLBuilder;
import io.cdap.plugin.gcp.bigquery.sqlengine.builder.BigQueryNestedSelectSQLBuilder;
import io.cdap.plugin.gcp.bigquery.sqlengine.builder.BigQuerySelectSQLBuilder;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * An implementation of {@link Relation} designed to operate on BigQuery.
 */
public class BigQueryRelation implements Relation {
  private static final SQLExpressionFactory factory = new SQLExpressionFactory();

  private final String datasetName;
  private final Set<String> columns;
  private final BigQuerySQLDataset sourceDataset;
  private final BigQueryRelation parent;
  private final String transformExpression;

  /**
   * Gets a new BigQueryRelation instance
   *
   * @param bqProject     Project where the source SQL table is stored
   * @param bqDataset     Dataset where the source SQL table is stored
   * @param sourceDataset source SQL Dataset instance
   * @param columnNames   column names to use when initializing this relation.
   * @return new BigQueryRelation instance for this table.
   */
  public static BigQueryRelation getInstance(String bqProject,
                                             String bqDataset,
                                             BigQuerySQLDataset sourceDataset,
                                             Set<String> columnNames) {
    Map<String, Expression> selectedColumns = getSelectedColumns(columnNames);
    // Build qualified source table and dataset names for base query.
    String sourceTable = String.format("%s.%s.%s", bqProject, bqDataset, sourceDataset.getBigQueryTableName());
    String datasetName = sourceDataset.getDatasetName();
    String transformExpression = buildBaseSelect(selectedColumns, sourceTable, datasetName);

    return new BigQueryRelation(datasetName, sourceDataset, columnNames, null, transformExpression);
  }

  @VisibleForTesting
  protected BigQueryRelation(String datasetName,
                           BigQuerySQLDataset sourceDataset,
                             Set<String> columns,
                             BigQueryRelation parent,
                             String transformExpression) {
    this.datasetName = datasetName;
    this.columns = columns;
    this.sourceDataset = sourceDataset;
    this.parent = parent;
    this.transformExpression = transformExpression;
  }

  private Relation getInvalidRelation(String validationError) {
    return new InvalidRelation(validationError);
  }

  @Override
  public boolean isValid() {
    return true;
  }

  @Override
  public String getValidationError() {
    return null;
  }

  public Relation getParent() {
    return parent;
  }

  public String getTransformExpression() {
    return transformExpression;
  }

  public Set<String> getColumns() {
    return columns;
  }

  public BigQuerySQLDataset getSourceDataset() {
    return sourceDataset;
  }

  public Relation setDatasetName(String datasetName) {
    return new BigQueryRelation(datasetName, sourceDataset, columns, this, transformExpression);
  }

  @Override
  public Relation setColumn(String column, Expression value) {
    // check if expression is supported and valid
    if (!supportsExpression(value)) {
      return getInvalidRelation("Unsupported or invalid expression type.");
    }

    Map<String, Expression> selectedColumns = getSelectedColumns(columns);
    selectedColumns.put(column, value);

    // Build new transform expression and return new instance.
    String expression = buildNestedSelect(selectedColumns, transformExpression, datasetName, null);
    return new BigQueryRelation(datasetName, sourceDataset, selectedColumns.keySet(), this, expression);
  }

  @Override
  public Relation dropColumn(String column) {
    // check if all expressions are supported and valid
    if (!columns.contains(column)) {
      return getInvalidRelation("Trying to remove non existing column in Relation: " + column);
    }

    // Remove column
    Map<String, Expression> selectedColumns = getSelectedColumns(columns);
    selectedColumns.remove(column);

    String expression = buildNestedSelect(selectedColumns, transformExpression, datasetName, null);
    return new BigQueryRelation(datasetName, sourceDataset, selectedColumns.keySet(), this, expression);
  }

  @Override
  public Relation select(Map<String, Expression> columns) {
    // check if all expressions are supported and valid
    if (!supportsExpressions(columns.values())) {
      return getInvalidRelation("Unsupported or invalid expression type.");
    }

    // Build new transform expression and return new instance.
    String expression = buildNestedSelect(columns, transformExpression, datasetName, null);
    return new BigQueryRelation(datasetName, sourceDataset, columns.keySet(), this, expression);
  }

  @Override
  public Relation filter(Expression filter) {
    // check if expression is supported and valid
    if (!supportsExpression(filter)) {
      return getInvalidRelation("Unsupported or invalid expression type.");
    }

    Map<String, Expression> selectedColumns = getSelectedColumns(columns);
    String expression = buildNestedSelect(selectedColumns, transformExpression, datasetName, filter);
    return new BigQueryRelation(datasetName, sourceDataset, columns, this, expression);
  }

  @Override
  public Relation groupBy(GroupByAggregationDefinition definition) {
    // Ensure all expressions supplied in this definition are supported and valid
    if (!supportsGroupByAggregationDefinition(definition)) {
      return getInvalidRelation("DeduplicateAggregationDefinition contains " +
                                  "unsupported or invalid expressions");
    }

    Set<String> columns = definition.getSelectExpressions().keySet();
    String expression = buildGroupBy(definition, transformExpression, datasetName);
    return new BigQueryRelation(datasetName, sourceDataset, columns, this, expression);
  }

  @Override
  public Relation deduplicate(DeduplicateAggregationDefinition definition) {
    // Ensure all expressions supplied in this definition are supported and valid
    if (!supportsDeduplicateAggregationDefinition(definition)) {
      return getInvalidRelation("DeduplicateAggregationDefinition contains " +
                                  "unsupported or invalid expressions");
    }

    Set<String> columns = definition.getSelectExpressions().keySet();
    String expression = buildDeduplicate(definition, transformExpression, datasetName);
    return new BigQueryRelation(datasetName, sourceDataset, columns, this, expression);
  }

  private static String buildBaseSelect(Map<String, Expression> columns,
                                        String sourceTable,
                                        String datasetName) {
    // We qualify all inputs when building the SQL query.
    BigQuerySelectSQLBuilder builder = new BigQuerySelectSQLBuilder(qualifyKeys(columns),
                                                                    qualify(sourceTable),
                                                                    qualify(datasetName),
                                                                    null);
    return builder.getQuery();
  }

  private static String buildNestedSelect(Map<String, Expression> columns,
                                          String sourceExpression,
                                          String datasetName,
                                          @Nullable Expression filter) {
    // Get filter conditions
    String filterCondition = filter != null ? ((SQLExpression) filter).extract() : null;

    // Instantiate query builder and generate select expression
    BigQueryNestedSelectSQLBuilder builder = new BigQueryNestedSelectSQLBuilder(qualifyKeys(columns),
                                                                                sourceExpression,
                                                                                qualify(datasetName),
                                                                                filterCondition);
    return builder.getQuery();
  }

  private static String buildGroupBy(GroupByAggregationDefinition definition,
                                     String sourceExpression,
                                     String datasetName) {
    // Instantiate query builder and generate select expression
    BigQueryGroupBySQLBuilder builder = new BigQueryGroupBySQLBuilder(qualify(definition),
                                                                      sourceExpression,
                                                                      datasetName);
    return builder.getQuery();
  }

  private static String buildDeduplicate(DeduplicateAggregationDefinition definition,
                                         String sourceExpression,
                                         String datasetName) {
    // Instantiate query builder and generate select expression
    BigQueryDeduplicateSQLBuilder builder = new BigQueryDeduplicateSQLBuilder(qualify(definition),
                                                                              sourceExpression,
                                                                              datasetName);
    return builder.getQuery();
  }

  /**
   * Use the {@link SQLExpressionFactory} to qualify identifiers (Columns names/aliases or table names/aliases)
   * @param identifier identifier to qualify
   * @return qualified identifier
   */
  private static String qualify(String identifier) {
    return factory.qualify(identifier);
  }

  /**
   * Transform a map containing alias -> column expression by qualifying the Alias.
   * @param columns map containing column aliases and expressions
   * @return Map with aliases qualified
   */
  private static Map<String, Expression> qualifyKeys(Map<String, Expression> columns) {
    // Keep the order of the original map
    Map<String, Expression> qualified = new LinkedHashMap<>();
    // We always qualify keys as we use them to build "column AS `key`"
    columns.forEach((k, v) -> qualified.put(qualify(k), v));
    return qualified;
  }

  /**
   * Builds selected columns map based on an input set of columns. Note that the column expression is qualified.
   * <p>
   * The output Map maintains the field order from the supplied set.
   *
   * @param columns set containing columns to select
   * @return Map containing column aliases and column values.
   */
  private static Map<String, Expression> getSelectedColumns(Set<String> columns) {
    Map<String, Expression> selectedColumns = new LinkedHashMap<>();
    columns.forEach(c -> selectedColumns.put(c, factory.compile(qualify(c))));
    return selectedColumns;
  }

  /**
   * Check if all expressions contained in a {@link GroupByAggregationDefinition} are supported.
   *
   * @param def {@link GroupByAggregationDefinition} to verify.
   * @return boolean specifying if all expressions are supported or not.
   */
  @VisibleForTesting
  protected static boolean supportsGroupByAggregationDefinition(GroupByAggregationDefinition def) {
    // Gets all expressions defined in this definition
    Collection<Expression> selectExpressions = def.getSelectExpressions().values();
    Collection<Expression> groupByExpressions = def.getGroupByExpressions();

    // Verify all supplied expressions are both supported and valid.
    return supportsExpressions(selectExpressions)
      && supportsExpressions(groupByExpressions);
  }

  /**
   * Builds a new {@link GroupByAggregationDefinition} with qualified aliases for the Select Expression.
   * @param def supplied {@link GroupByAggregationDefinition}
   * @return {@link GroupByAggregationDefinition} with qualified column aliases
   */
  protected static GroupByAggregationDefinition qualify(GroupByAggregationDefinition def) {
    GroupByAggregationDefinition.Builder builder = GroupByAggregationDefinition.builder();
    builder.select(qualifyKeys(def.getSelectExpressions()));
    builder.groupBy(def.getGroupByExpressions());
    return builder.build();
  }

  /**
   * Check if all expressions contained in a {@link DeduplicateAggregationDefinition} are supported.
   *
   * @param def {@link DeduplicateAggregationDefinition} to verify.
   * @return boolean specifying if all expressions are supported or not.
   */
  @VisibleForTesting
  protected static boolean supportsDeduplicateAggregationDefinition(DeduplicateAggregationDefinition def) {
    // Gets all expressions defined in this definition
    Collection<Expression> selectExpressions = def.getSelectExpressions().values();
    Collection<Expression> dedupExpressions = def.getGroupByExpressions();
    Collection<Expression> orderExpressions = def.getFilterExpressions()
      .stream()
      .map(DeduplicateAggregationDefinition.FilterExpression::getExpression)
      .collect(Collectors.toSet());

    // Verify all supplied expressions are both supported and valid.
    return supportsExpressions(selectExpressions)
      && supportsExpressions(dedupExpressions)
      && supportsExpressions(orderExpressions);
  }

  /**
   * Builds a new {@link DeduplicateAggregationDefinition} with qualified aliases for the Select Expression.
   * @param def supplied {@link DeduplicateAggregationDefinition}
   * @return {@link DeduplicateAggregationDefinition} with qualified column aliases
   */
  protected static DeduplicateAggregationDefinition qualify(DeduplicateAggregationDefinition def) {
    DeduplicateAggregationDefinition.Builder builder = DeduplicateAggregationDefinition.builder();
    builder.select(qualifyKeys(def.getSelectExpressions()));
    builder.dedupOn(def.getGroupByExpressions());
    builder.filterDuplicatesBy(def.getFilterExpressions());
    return builder.build();
  }

  /**
   * Check if a collection of expressions are all valid
   *
   * @param expressions collection containing expressions to verify
   * @return boolean specifying if all expressions are supported or not.
   */
  @VisibleForTesting
  protected static boolean supportsExpressions(Collection<Expression> expressions) {
    return expressions.stream().allMatch(BigQueryRelation::supportsExpression);
  }

  /**
   * Check if an expression is valid
   *
   * @param expression expression to verity
   * @return boolean specifying if the expression is supported and valid.
   */
  @VisibleForTesting
  protected static boolean supportsExpression(Expression expression) {
    return expression instanceof SQLExpression && expression.isValid();
  }

}
