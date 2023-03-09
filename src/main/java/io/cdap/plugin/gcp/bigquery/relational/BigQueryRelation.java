package io.cdap.plugin.gcp.bigquery.relational;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.feature.FeatureFlagsProvider;
import io.cdap.cdap.etl.api.aggregation.DeduplicateAggregationDefinition;
import io.cdap.cdap.etl.api.aggregation.GroupByAggregationDefinition;
import io.cdap.cdap.etl.api.aggregation.WindowAggregationDefinition;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.features.Feature;
import io.cdap.plugin.gcp.bigquery.sqlengine.BigQuerySQLDataset;
import io.cdap.plugin.gcp.bigquery.sqlengine.builder.BigQueryDeduplicateSQLBuilder;
import io.cdap.plugin.gcp.bigquery.sqlengine.builder.BigQueryGroupBySQLBuilder;
import io.cdap.plugin.gcp.bigquery.sqlengine.builder.BigQueryNestedSelectSQLBuilder;
import io.cdap.plugin.gcp.bigquery.sqlengine.builder.BigQuerySelectSQLBuilder;
import io.cdap.plugin.gcp.bigquery.sqlengine.builder.BigQueryWindowsAggregationSQLBuilder;
import org.apache.parquet.Strings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * An implementation of {@link Relation} designed to operate on BigQuery.
 */
public class BigQueryRelation implements Relation {
  private static final SQLExpressionFactory factory = new SQLExpressionFactory();

  private final FeatureFlagsProvider featureFlagsProvider;
  private final String datasetName;
  private final Set<String> columns;
  private final BigQueryRelation parent;
  private final Supplier<String> sqlStatementSupplier;
  private final Schema schema;

  private Map<String, BigQuerySQLDataset> sourceDatasets;

  /**
   * Gets a new BigQueryRelation instance
   *
   * @param datasetName source dataset name
   * @param columnNames column names to use when initializing this relation.
   * @return new BigQueryRelation instance for this table.
   */
  public static BigQueryRelation getInstance(String datasetName,
                                             Set<String> columnNames,
                                             FeatureFlagsProvider featureFlagsProvider) {
    return new BigQueryRelation(datasetName, columnNames, featureFlagsProvider);
  }

  /**
   * Gets a new BigQueryRelation instance with a schema supplied
   *
   * @param datasetName source dataset name
   * @param columnNames column names to use when initializing this relation.
   * @param schema the CDAP representation of the schema of the relation.
   * @return new BigQueryRelation instance for this table.
   */
  public static BigQueryRelation getInstance(String datasetName,
                                             Set<String> columnNames,
                                             FeatureFlagsProvider featureFlagsProvider,
                                             @Nullable Schema schema) {
    return new BigQueryRelation(datasetName, columnNames, featureFlagsProvider, schema);
  }

  @VisibleForTesting
  protected BigQueryRelation(String datasetName,
                             Set<String> columns,
                             FeatureFlagsProvider featureFlagsProvider) {
    this(datasetName, columns, featureFlagsProvider, null);
  }

  @VisibleForTesting
  protected BigQueryRelation(String datasetName,
                             Set<String> columns,
                             FeatureFlagsProvider featureFlagsProvider,
                             @Nullable Schema schema) {
    this.datasetName = datasetName;
    this.columns = columns;
    this.featureFlagsProvider = featureFlagsProvider;
    this.schema = schema;
    this.parent = null;
    this.sqlStatementSupplier = () -> {

      // Check if Dataset exists
      BigQuerySQLDataset sourceDataset = sourceDatasets.get(datasetName);
      if (sourceDataset == null) {
        throw new SQLEngineException("Unable to find dataset with name " + datasetName);
      }

      // Build selected columns for this dataset based on initial columns present in relation.
      Map<String, Expression> selectedColumns = getSelectedColumns(columns);
      // Build source table identifier using the Project, Dataset and Table
      String sourceTable = String.format("%s.%s.%s",
                                         sourceDataset.getBigQueryProject(),
                                         sourceDataset.getBigQueryDataset(),
                                         sourceDataset.getBigQueryTable());
      // Build initial select from the source table.
      return buildBaseSelect(selectedColumns, sourceTable, datasetName);
    };
  }

  @VisibleForTesting
  protected BigQueryRelation(String datasetName,
                             Set<String> columns,
                             FeatureFlagsProvider featureFlagsProvider,
                             @Nullable BigQueryRelation parent,
                             Supplier<String> sqlStatementSupplier) {
    this(datasetName, columns, featureFlagsProvider, parent, sqlStatementSupplier, null);
  }

  @VisibleForTesting
  protected BigQueryRelation(String datasetName,
                             Set<String> columns,
                             FeatureFlagsProvider featureFlagsProvider,
                             @Nullable BigQueryRelation parent,
                             Supplier<String> sqlStatementSupplier,
                             @Nullable Schema schema) {
    this.datasetName = datasetName;
    this.columns = columns;
    this.featureFlagsProvider = featureFlagsProvider;
    this.parent = parent;
    this.sqlStatementSupplier = sqlStatementSupplier;
    this.schema = schema;
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

  /**
   * Get parent relation
   *
   * @return parent relation instance. This can be null for a base relation.
   */
  @Nullable
  public Relation getParent() {
    return parent;
  }

  /**
   * Method use to materialize the transform expression from this dataset.
   *
   * @return transform expression used when executing SQL statements.
   */
  public String getSQLStatement() {
    return sqlStatementSupplier.get();
  }

  /**
   * Get columns defined in this relation
   *
   * @return Columns defined in this relation.
   */
  public Set<String> getColumns() {
    return columns;
  }

  /**
   * Get the {@link Schema} of the relation if available, null otherwise.
   *
   * @return the schema of the relation.
   */
  @Nullable
  public Schema getSchema() {
    return schema;
  }

  /**
   * Sets input datasets for this instance and the parent instance (if defined)
   */
  public void setInputDatasets(Map<String, BigQuerySQLDataset> datasets) {
    this.sourceDatasets = datasets;

    // Propagate datasets into parent.
    if (parent != null) {
      parent.setInputDatasets(datasets);
    }
  }

  /**
   * Get the dataset name for this relation.
   *
   * @return dataset name
   */
  public String getDatasetName() {
    return datasetName;
  }

  /**
   * Get a new relation with a redefined dataset name.
   *
   * @param newDatasetName new dataset name for this relation.
   * @return new Relation with the new dataset name.
   */
  public Relation setDatasetName(String newDatasetName) {
    Map<String, Expression> selectedColumns = getSelectedColumns(columns);
    // Build new transform expression and return new instance.
    Supplier<String> supplier =
      () -> buildNestedSelect(selectedColumns, getSQLStatement(), newDatasetName, null);
    return new BigQueryRelation(newDatasetName, columns, featureFlagsProvider, this, supplier);
  }

  @Override
  public Relation setColumn(String column, Expression value) {
    // check if expression is supported and valid
    if (!supportsExpression(value)) {
      return getInvalidRelation("Unsupported or invalid expression type : "
                                  + getInvalidExpressionCause(value));
    }

    Map<String, Expression> selectedColumns = getSelectedColumns(columns);
    selectedColumns.put(column, value);

    // Build new transform expression and return new instance.
    Supplier<String> supplier =
      () -> buildNestedSelect(selectedColumns, getSQLStatement(), datasetName, null);
    return new BigQueryRelation(datasetName, selectedColumns.keySet(), featureFlagsProvider, this, supplier);
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

    // Build new transform expression and return new instance.
    Supplier<String> supplier =
      () -> buildNestedSelect(selectedColumns, getSQLStatement(), datasetName, null);
    return new BigQueryRelation(datasetName, selectedColumns.keySet(), featureFlagsProvider, this, supplier);
  }

  @Override
  public Relation select(Map<String, Expression> columns) {
    // check if all expressions are supported and valid
    if (!supportsExpressions(columns.values())) {
      return getInvalidRelation("Unsupported or invalid expression type: "
                                  + getInvalidExpressionCauses(columns.values()));
    }

    // Build new transform expression and return new instance.
    Supplier<String> supplier =
      () -> buildNestedSelect(columns, getSQLStatement(), datasetName, null);
    return new BigQueryRelation(datasetName, columns.keySet(), featureFlagsProvider, this, supplier);
  }

  @Override
  public Relation filter(Expression filter) {
    // check if expression is supported and valid
    if (!supportsExpression(filter)) {
      return getInvalidRelation("Unsupported or invalid expression type: "
                                  + getInvalidExpressionCause(filter));
    }

    Map<String, Expression> selectedColumns = getSelectedColumns(columns);
    // Build new transform expression and return new instance.
    Supplier<String> supplier =
      () -> buildNestedSelect(selectedColumns, getSQLStatement(), datasetName, filter);
    return new BigQueryRelation(datasetName, columns, featureFlagsProvider, this, supplier);
  }

  @Override
  public Relation groupBy(GroupByAggregationDefinition definition) {
    // Check if group by feature is enabled
    if (!Feature.PUSHDOWN_TRANSFORMATION_GROUPBY.isEnabled(featureFlagsProvider)) {
      return getInvalidRelation(String.format("Feature %s is not enabled.",
                                              Feature.PUSHDOWN_TRANSFORMATION_GROUPBY.getFeatureFlagString()));
    }

    // Ensure all expressions supplied in this definition are supported and valid
    if (!supportsGroupByAggregationDefinition(definition)) {
      return getInvalidRelation("DeduplicateAggregationDefinition contains "
                                  + "unsupported or invalid expressions: "
                                  + collectGroupByAggregationDefinitionErrors(definition));
    }

    Set<String> columns = definition.getSelectExpressions().keySet();

    // Build new transform expression and return new instance.
    Supplier<String> supplier =
      () -> buildGroupBy(definition, getSQLStatement(), datasetName);
    return new BigQueryRelation(datasetName, columns, featureFlagsProvider, this, supplier);
  }

  @Override
  public Relation deduplicate(DeduplicateAggregationDefinition definition) {
    // Check if deduplicate feature is enabled
    if (!Feature.PUSHDOWN_TRANSFORMATION_DEDUPLICATE.isEnabled(featureFlagsProvider)) {
      return getInvalidRelation(String.format("Feature %s is not enabled.",
                                              Feature.PUSHDOWN_TRANSFORMATION_DEDUPLICATE.getFeatureFlagString()));
    }

    // Ensure all expressions supplied in this definition are supported and valid
    if (!supportsDeduplicateAggregationDefinition(definition)) {
      return getInvalidRelation("DeduplicateAggregationDefinition contains "
                                  + "unsupported or invalid expressions: "
                                  + collectDeduplicateAggregationDefinitionErrors(definition));
    }

    Set<String> columns = definition.getSelectExpressions().keySet();
    Supplier<String> supplier =
      () -> buildDeduplicate(definition, getSQLStatement(), datasetName);
    return new BigQueryRelation(datasetName, columns, featureFlagsProvider, this, supplier);
  }

  @Override
  public Relation window(WindowAggregationDefinition definition) {
    // Check if window feature is enabled
    if (!Feature.PUSHDOWN_TRANSFORMATION_WINDOWAGGREGATION.isEnabled(featureFlagsProvider)) {
      return getInvalidRelation(String.format("Feature %s is not enabled.",
        Feature.PUSHDOWN_TRANSFORMATION_WINDOWAGGREGATION.getFeatureFlagString()));
    }

    // Ensure all expressions supplied in this definition are supported and valid
    if (!supportsWindowAggregationDefinition(definition)) {
      return getInvalidRelation("WindowAggregationDefinition contains "
        + "unsupported or invalid expressions: "
        + collectWindowAggregationDefinitionErrors(definition));
    }

    Set<String> columns = definition.getSelectExpressions().keySet();
    Supplier<String> supplier =
      () -> buildWindow(definition, getSQLStatement(), datasetName);
    return new BigQueryRelation(datasetName, columns, featureFlagsProvider, this, supplier);
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
                                                                      qualify(datasetName));
    return builder.getQuery();
  }

  private static String buildDeduplicate(DeduplicateAggregationDefinition definition,
                                         String sourceExpression,
                                         String datasetName) {
    // Instantiate query builder and generate select expression
    BigQueryDeduplicateSQLBuilder builder = new BigQueryDeduplicateSQLBuilder(qualify(definition),
                                                                              sourceExpression,
                                                                              qualify(datasetName));
    return builder.getQuery();
  }

  private static String buildWindow(WindowAggregationDefinition definition,
                                    String sourceExpression,
                                    String datasetName) {
    BigQueryWindowsAggregationSQLBuilder builder = new BigQueryWindowsAggregationSQLBuilder(qualify(definition),
                                                                                            sourceExpression,
                                                                                            qualify(datasetName));
    return builder.getQuery();
  }

  /**
   * Use the {@link SQLExpressionFactory} to qualify identifiers (Columns names/aliases or table names/aliases)
   *
   * @param identifier identifier to qualify
   * @return qualified identifier
   */
  private static String qualify(String identifier) {
    return factory.qualify(identifier);
  }

  /**
   * Transform a map containing alias -> column expression by qualifying the Alias.
   *
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
   * Check if all expressions contained in a {@link GroupByAggregationDefinition} are supported.
   *
   * @param def {@link GroupByAggregationDefinition} to verify.
   * @return boolean specifying if all expressions are supported or not.
   */
  @VisibleForTesting
  @Nullable
  protected static String collectGroupByAggregationDefinitionErrors(GroupByAggregationDefinition def) {
    // If the expression is valid, this must be null.
    if (supportsGroupByAggregationDefinition(def)) {
      return null;
    }

    // Gets all expressions defined in this definition
    Collection<Expression> selectExpressions = def.getSelectExpressions().values();
    Collection<Expression> groupByExpressions = def.getGroupByExpressions();

    // Get all invalid expression causes, and prepend field origin to error reasons
    String selectErrors = getInvalidExpressionCauses(selectExpressions);
    if (!Strings.isNullOrEmpty(selectErrors)) {
      selectErrors = "Select fields: " + selectErrors;
    }

    String groupByErrors = getInvalidExpressionCauses(groupByExpressions);
    if (!Strings.isNullOrEmpty(groupByErrors)) {
      groupByErrors = "Grouping fields: " + groupByErrors;
    }

    // Build string which concatenates all non-empty error groups, separated by a hyphen.
    return Stream.of(selectErrors, groupByErrors)
      .filter(Objects::nonNull)
      .collect(Collectors.joining(" - "));
  }

  /**
   * Builds a new {@link GroupByAggregationDefinition} with qualified aliases for the Select Expression.
   *
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
   * Check if all expressions contained in a {@link DeduplicateAggregationDefinition} are supported.
   *
   * @param def {@link DeduplicateAggregationDefinition} to verify.
   * @return boolean specifying if all expressions are supported or not.
   */
  @VisibleForTesting
  @Nullable
  protected static String collectDeduplicateAggregationDefinitionErrors(DeduplicateAggregationDefinition def) {
    // If the expression is valid, this must be null.
    if (supportsDeduplicateAggregationDefinition(def)) {
      return null;
    }

    // Gets all expressions defined in this definition
    Collection<Expression> selectExpressions = def.getSelectExpressions().values();
    Collection<Expression> dedupExpressions = def.getGroupByExpressions();
    Collection<Expression> orderExpressions = def.getFilterExpressions()
      .stream()
      .map(DeduplicateAggregationDefinition.FilterExpression::getExpression)
      .collect(Collectors.toSet());

    // Get all invalid expression causes, and prepend origin to error reasons
    String selectErrors = getInvalidExpressionCauses(selectExpressions);
    if (!Strings.isNullOrEmpty(selectErrors)) {
      selectErrors = "Select fields: " + selectErrors;
    }

    String dedupErrors = getInvalidExpressionCauses(dedupExpressions);
    if (!Strings.isNullOrEmpty(dedupErrors)) {
      dedupErrors = "Deduplication fields: " + dedupErrors;
    }

    String orderErrors = getInvalidExpressionCauses(orderExpressions);
    if (!Strings.isNullOrEmpty(orderErrors)) {
      orderErrors = "Order fields: " + orderErrors;
    }

    // Build string which concatenates all non-empty error groups, separated by a hyphen.
    return Stream.of(selectErrors, dedupErrors, orderErrors)
      .filter(Objects::nonNull)
      .collect(Collectors.joining(" - "));
  }

  /**
   * Builds a new {@link DeduplicateAggregationDefinition} with qualified aliases for the Select Expression.
   *
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

  protected static boolean supportsWindowAggregationDefinition(WindowAggregationDefinition def) {
    Collection<Expression> partitionExpressions =  def.getPartitionExpressions();
    Collection<Expression> selectExpressions = def.getSelectExpressions().values();
    Collection<Expression> orderExpressions = getWindowAggregationOrderByExpression(def);
    return supportsExpressions(partitionExpressions) && supportsExpressions(selectExpressions)
      && supportsExpressions(orderExpressions);
  }

  private static Collection<Expression> getWindowAggregationOrderByExpression(WindowAggregationDefinition def) {
    Collection<Expression> orderExpressions = new ArrayList<>(def.getOrderByExpressions().size());
    for (WindowAggregationDefinition.OrderByExpression orderByExpression : def.getOrderByExpressions()) {
      orderExpressions.add(orderByExpression.getExpression());
    }
    return orderExpressions;
  }

  @VisibleForTesting
  protected static WindowAggregationDefinition qualify(WindowAggregationDefinition def) {
    return WindowAggregationDefinition.builder().select(qualifyKeys(def.getSelectExpressions()))
      .partition(def.getPartitionExpressions()).aggregate(def.getAggregateExpressions())
      .orderBy(def.getOrderByExpressions()).windowFrameType(def.getWindowFrameType())
      .unboundedFollowing(def.getUnboundedFollowing()).unboundedPreceding(def.getUnboundedPreceding())
      .following(def.getFollowing()).preceding(def.getPreceding()).build();
  }

  @VisibleForTesting
  @Nullable
  protected static String collectWindowAggregationDefinitionErrors(WindowAggregationDefinition def) {
    // If the expression is valid, this must be null.
    if (supportsWindowAggregationDefinition(def)) {
      return null;
    }

    // Gets all expressions defined in this definition
    Collection<WindowAggregationDefinition.OrderByExpression> orderByExpressions = def.getOrderByExpressions();
    Collection<Expression> orderExpressions = getWindowAggregationOrderByExpression(def);
    // Get all invalid expression causes, and prepend field origin to error reasons
    String selectErrors = getInvalidExpressionCauses(def.getSelectExpressions().values());
    if (!Strings.isNullOrEmpty(selectErrors)) {
      selectErrors = "Select fields: " + selectErrors;
    }

    String partitionErrors = getInvalidExpressionCauses(def.getPartitionExpressions());
    if (!Strings.isNullOrEmpty(partitionErrors)) {
      partitionErrors = "Window fields: " + partitionErrors;
    }

    String orderErrors = getInvalidExpressionCauses(orderExpressions);
    if (!Strings.isNullOrEmpty(orderErrors)) {
      orderErrors = "Order fields: " + orderErrors;
    }

    // Build string which concatenates all non-empty error groups, separated by a hyphen.
    return Stream.of(selectErrors, partitionErrors, orderErrors)
      .filter(Objects::nonNull)
      .collect(Collectors.joining(" - "));
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

  /**
   * Collects validation errors for a colleciton of expressions
   *
   * @param expressions collection containing expressions to verify
   * @return boolean specifying if all expressions are supported or not.
   */
  @VisibleForTesting
  @Nullable
  protected static String getInvalidExpressionCauses(Collection<Expression> expressions) {
    // If the expressions are valid, this is null.
    if (supportsExpressions(expressions)) {
      return null;
    }

    // Collect failure reasons
    return expressions.stream()
      .map(BigQueryRelation::getInvalidExpressionCause)
      .filter(Objects::nonNull)
      .collect(Collectors.joining(" ; "));
  }

  /**
   * Gets the validation error for an expression
   *
   * @param expression expression to verity
   * @return Validation error for this expression, or null if the expression is valid.
   */
  @VisibleForTesting
  @Nullable
  protected static String getInvalidExpressionCause(Expression expression) {
    // If the expressions are valid, this is null.
    if (supportsExpression(expression)) {
      return null;
    }

    // Null expressions are not supported
    if (expression == null) {
      return "Expression is null";
    }

    // We only support SQLExpression
    if (!(expression instanceof SQLExpression)) {
      return "Unsupported Expression type \"" + expression.getClass().getCanonicalName() + "\"";
    }

    // Return validation error for this expression.
    return expression.getValidationError() != null ? expression.getValidationError() : "Unknown";
  }
}
