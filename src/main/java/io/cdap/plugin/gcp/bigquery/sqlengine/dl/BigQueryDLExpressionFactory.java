package io.cdap.plugin.gcp.bigquery.sqlengine.dl;

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.etl.api.dl.DLExpression;
import io.cdap.cdap.etl.api.dl.DLExpressionCapability;
import io.cdap.cdap.etl.api.dl.DLExpressionFactory;
import io.cdap.cdap.etl.api.engine.sql.StandardSQLCapabilities;
import java.util.Set;

/**
 * A DL expression factory that provides access to simple SQL expressions
 */
public class BigQueryDLExpressionFactory implements DLExpressionFactory {
  private static final Set<DLExpressionCapability> CAPABILITIES = ImmutableSet.of(
      StandardSQLCapabilities.SQL
      //TODO: add core capabilities
  );

  @Override
  public Set<DLExpressionCapability> getCapabilities() {
    return CAPABILITIES;
  }

  @Override
  public DLExpression compile(String expression) {
    return new BigQueryDLExpression(expression);
  }
}
