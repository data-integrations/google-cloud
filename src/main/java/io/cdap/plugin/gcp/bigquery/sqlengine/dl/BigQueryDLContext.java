package io.cdap.plugin.gcp.bigquery.sqlengine.dl;

import io.cdap.cdap.etl.api.dl.DLContext;
import io.cdap.cdap.etl.api.dl.DLExpressionFactory;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of {@link DLContext} that provides access to simple SQL expressions.
 */
public class BigQueryDLContext implements DLContext {
  private static final List<DLExpressionFactory> FACTORIES = Collections.singletonList(
      new BigQueryDLExpressionFactory()
  );

  @Override
  public List<DLExpressionFactory> getExpressionFactories() {
    return FACTORIES;
  }
}
