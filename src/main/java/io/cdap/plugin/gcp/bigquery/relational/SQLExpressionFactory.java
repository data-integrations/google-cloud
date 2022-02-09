package io.cdap.plugin.gcp.bigquery.relational;

import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.ExpressionFactoryType;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * An {@link ExpressionFactory} that compiles SQL strings into expressions.
 * The resultant expressions are of type {@link SQLExpression}.
 */
public class SQLExpressionFactory implements ExpressionFactory<String> {

  /**
   * Gets the expression factory type, which in this case is SQL.
   * @return {@link StringExpressionFactoryType}.SQL.
   */
  @Override
  public ExpressionFactoryType<String> getType() {
    return StringExpressionFactoryType.SQL;
  }

  /**
   * Saves the SQL expression specified in a {@link SQLExpression} and returns it.
   * @param expression A valid SQL string with which an Expression can be created.
   * @return The compiled {@link SQLExpression}.
   */
  @Override
  public Expression compile(String expression) {
    return new SQLExpression(expression);
  }

  /**
   * Get the set of Capabilities supported, which in this case is SQL.
   * @return A single capability, {@link StringExpressionFactoryType}.SQL.
   */
  @Override
  public Set<Capability> getCapabilities() {
    return new HashSet<>(Collections.singleton(StringExpressionFactoryType.SQL));
  }
}
