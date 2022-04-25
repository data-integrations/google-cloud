package io.cdap.plugin.gcp.bigquery.source;

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class BigQuerySourceConfigTest {

  @Test
  public void testValidatePartitionDateWithInvalidDate() throws NoSuchMethodException, InvocationTargetException,
    IllegalAccessException {
    MockFailureCollector collector = new MockFailureCollector();
    BigQuerySourceConfig config = BigQuerySourceConfig.builder().build();
    Method method = config.getClass().getDeclaredMethod("validatePartitionDate", FailureCollector.class,
                                                        String.class, String.class);
    method.setAccessible(true);
    String userEntryDate = "10-2022-10";
    String fieldName = "partitionFrom";
    method.invoke(config, collector, userEntryDate, fieldName);
    Assert.assertEquals(String.format("%s is not in a valid format.", userEntryDate, fieldName),
                        collector.getValidationFailures().stream().findFirst().get().getMessage());
  }

  @Test
  public void testValidatePartitionDateWithValidDate() throws NoSuchMethodException, InvocationTargetException,
    IllegalAccessException {
    MockFailureCollector collector = new MockFailureCollector();
    BigQuerySourceConfig config = BigQuerySourceConfig.builder().build();
    Method method = config.getClass().getDeclaredMethod("validatePartitionDate", FailureCollector.class,
                                                        String.class, String.class);
    method.setAccessible(true);
    String userEntryDate = "2022-10-10";
    String fieldName = "partitionFrom";
    method.invoke(config, collector, userEntryDate, fieldName);
    Assert.assertEquals(0,
                        collector.getValidationFailures().size());
  }
}
