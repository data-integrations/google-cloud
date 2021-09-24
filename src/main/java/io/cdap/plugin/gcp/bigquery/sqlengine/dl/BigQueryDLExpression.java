package io.cdap.plugin.gcp.bigquery.sqlengine.dl;

import io.cdap.cdap.etl.api.dl.DLExpression;

/**
 * A very simple DL expression object that simply holds an expression string to be embedded in SQL
 */
public class BigQueryDLExpression implements DLExpression {
  private final String expression;

  public BigQueryDLExpression(String expression) {
    this.expression = expression;
  }

  @Override
  public String toString() {
    return expression;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BigQueryDLExpression)) {
      return false;
    }
    BigQueryDLExpression that = (BigQueryDLExpression) o;
    return expression.equals(that.expression);
  }

  @Override
  public int hashCode() {
    return expression.hashCode();
  }
}
