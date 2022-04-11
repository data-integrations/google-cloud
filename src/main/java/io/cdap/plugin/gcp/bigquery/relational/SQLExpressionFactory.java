package io.cdap.plugin.gcp.bigquery.relational;

import io.cdap.cdap.etl.api.engine.sql.StandardSQLCapabilities;
import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.CoreExpressionCapabilities;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.ExpressionFactoryType;
import io.cdap.cdap.etl.api.relational.ExtractableExpression;
import io.cdap.cdap.etl.api.relational.InvalidExtractableExpression;
import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;
import io.cdap.plugin.gcp.bigquery.sqlengine.builder.BigQueryBaseSQLBuilder;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * An {@link ExpressionFactory} that compiles SQL strings into expressions.
 * The resultant expressions are of type {@link SQLExpression}.
 */
public class SQLExpressionFactory implements ExpressionFactory<String> {

  private static final Set<Capability> CAPABILITIES = Collections.unmodifiableSet(
    new HashSet<Capability>() {{
      add(StringExpressionFactoryType.SQL);
      add(StandardSQLCapabilities.BIGQUERY);
      add(CoreExpressionCapabilities.CAN_GET_QUALIFIED_DATASET_NAME);
      add(CoreExpressionCapabilities.CAN_GET_QUALIFIED_COLUMN_NAME);
      add(CoreExpressionCapabilities.CAN_SET_DATASET_ALIAS);
    }});

  /**
   * Gets the expression factory type, which in this case is SQL.
   *
   * @return {@link StringExpressionFactoryType}.SQL.
   */
  @Override
  public ExpressionFactoryType<String> getType() {
    return StringExpressionFactoryType.SQL;
  }

  /**
   * Saves the SQL expression specified in a {@link SQLExpression} and returns it.
   *
   * @param expression A valid SQL string with which an Expression can be created.
   * @return The compiled {@link SQLExpression}.
   */
  @Override
  public Expression compile(String expression) {
    return new SQLExpression(expression);
  }

  /**
   * Get the set of Capabilities supported, which in this case is SQL.
   *
   * @return a set containing all capabilities supported by this SQL engine.
   */
  @Override
  public Set<Capability> getCapabilities() {
    return CAPABILITIES;
  }

  /**
   * Returns qualified dataset name for an expression. For BigQuery, dataset names are wrapped in quotes (`).
   * The resulting expression will be invalid if the relation is not a {@link BigQueryRelation}.
   * @param relation supplied relation.
   * @return valid containing the column name wrapped in quotes; or invalid expression.
   */
  @Override
  public ExtractableExpression<String> getQualifiedDataSetName(Relation relation) {
    // Ensure relation is BigQueryRelation
    if (!(relation instanceof BigQueryRelation)) {
      return new InvalidExtractableExpression<>("Relation is not BigQueryRelation");
    }

    // Return Dataset name wrapped in quotes.
    String datasetName = ((BigQueryRelation) relation).getDatasetName();
    return new SQLExpression(qualify(datasetName));
  }

  /**
   * Returns qualified dataset column for a column. For BigQuery, dataset names are wrapped in quotes (`).
   * The resulting expression will be invalid if the relation is not a {@link BigQueryRelation}
   * or the column does not exist in this relation.
   * @param relation supplied relation.
   * @param column   column name.
   * @return valid containing the column name wrapped in quotes; or invalid expression.
   */
  @Override
  public ExtractableExpression<String> getQualifiedColumnName(Relation relation, String column) {
    // Ensure relation is BigQueryRelation
    if (!(relation instanceof BigQueryRelation)) {
      return new InvalidExtractableExpression<>("Relation is not BigQueryRelation");
    }

    BigQueryRelation bqRelation = (BigQueryRelation) relation;

    // Ensure column is present in relation.
    if (!bqRelation.getColumns().contains(column)) {
      return new InvalidExtractableExpression<>("Column " + column + " is not present in dataset");
    }

    return new SQLExpression(qualify(column));
  }

  @Override
  public Relation setDataSetAlias(Relation relation, String alias) {    // Ensure relation is BigQueryRelation
    if (!(relation instanceof BigQueryRelation)) {
      return new InvalidRelation("Alias cannot be set when the relation is not BigQueryRelation");
    }

    BigQueryRelation bqRelation = (BigQueryRelation) relation;
    return bqRelation.setDatasetName(alias);
  }

  /**
   * Method used to build a qualified identified in BigQuery
   * @param identifier identifier
   * @return Identifier wrapped in quotes (`)
   */
  public String qualify(String identifier) {
    return BigQueryBaseSQLBuilder.QUOTE + identifier + BigQueryBaseSQLBuilder.QUOTE;
  }
}
