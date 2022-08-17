package io.cdap.plugin.gcp.bigquery.relational;

import io.cdap.cdap.etl.api.relational.ExtractableExpression;

import java.util.Objects;

/**
 * A default implementation of Expression that simply stores the expression as a string.
 * The string is assumed to be ANSI SQL compliant.
 * It is the responsibility of the factory creating {@link SQLExpression} objects to enforce correctness of
 * the expression.
 */
public class SQLExpression implements ExtractableExpression<String> {
  private final String expression;

  /**
   * Creates a {@link SQLExpression} from the specified SQL string.
   * @param expression a String containing the SQL expression.
   */
  public SQLExpression(String expression) {
    this.expression = expression;
  }

  /**
   *
   * @return the SQL string contained in the {@link SQLExpression} object.
   */
  @Override
  public String extract() {
    return expression;
  }

  /**
   * A {@link SQLExpression} is always assumed to contain valid SQL. It is the responsibility of the creator of the
   * object to ensure correctness of the SQL string.
   * @return This method always returns true.
   */
  @Override
  public boolean isValid() {
    return true;
  }

  /**
   * A {@link SQLExpression} is always assumed to contain valid SQL. It is the responsibility of the creator of the
   * object to ensure correctness of the SQL string.
   * @return This method always returns a null string.
   */
  @Override
  public String getValidationError() {
    return null;
  }

  /**
   * Two {@link SQLExpression} objects are considered equal if they contain equal SQL expressions.
   * @param o other object to be compared for equality.
   * @return true iff both objects are {@link SQLExpression}s with equal SQL strings.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SQLExpression that = (SQLExpression) o;
    return Objects.equals(extract(), that.extract());
  }

  @Override
  public int hashCode() {
    return Objects.hash(extract());
  }
}
