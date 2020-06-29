package io.cdap.plugin.gcp.bigquery.action;

import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

public class BigQueryArgumentSetterConfigTest {

  private static final String VALID_REF = "ref";
  private static final String VALID_DATASET = "dataset";
  private static final String VALID_TABLE = "table";
  private static final String VALID_ARGUMENT_SELECTION_CONDITIONS = "feed=10;id=0";
  private static final String VALID_ARGUMENT_COLUMN = "name";

  @Test
  public void testValidateMissingArgumentSelectionConditions() {
    BigQueryArgumentSetterConfig config = getBuilder().setArgumentSelectionConditions(null).build();
    validateConfigValidationFail(config);
  }

  @Test
  public void testValidateMissingArgumentColumns() {
    BigQueryArgumentSetterConfig config = getBuilder().setArgumentsColumns(null).build();
    validateConfigValidationFail(config);
  }

  private static BigQueryArgumentSetterConfigBuilder getBuilder() {
    return BigQueryArgumentSetterConfigBuilder.bigQueryArgumentSetterConfig()
        .setReferenceName(VALID_REF)
        .setDataset(VALID_DATASET)
        .setTable(VALID_TABLE)
        .setArgumentSelectionConditions(VALID_ARGUMENT_SELECTION_CONDITIONS)
        .setArgumentsColumns(VALID_ARGUMENT_COLUMN);
  }

  private static void validateConfigValidationFail(BigQueryArgumentSetterConfig config) {
    MockFailureCollector collector = new MockFailureCollector();
    try {
      config.validateProperties(collector);
      Assert.assertEquals(1, collector.getValidationFailures().size());
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
    }
  }
}
