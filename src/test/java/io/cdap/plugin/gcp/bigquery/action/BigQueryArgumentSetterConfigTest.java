/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.bigquery.action;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.common.collect.ImmutableList;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class BigQueryArgumentSetterConfigTest {

  private static final String VALID_DATASET = "dataset";
  private static final String VALID_DATASET_PROJECT = "datasetProject";
  private static final String VALID_TABLE = "table";
  private static final String VALID_ARGUMENT_SELECTION_CONDITIONS = "feed=10;id=0";
  private static final String VALID_ARGUMENT_COLUMN = "name";
  private MockFailureCollector mockFailureCollector;

  @Before
  public void setUp() {
    mockFailureCollector = new MockFailureCollector();
  }

  @Test
  public void testValidateMissingArgumentSelectionConditions() {
    BigQueryArgumentSetterConfig config = getBuilder().setArgumentSelectionConditions(null).build();
    validateConfigValidationFail(config, BigQueryArgumentSetterConfig.NAME_ARGUMENT_SELECTION_CONDITIONS);
  }

  @Test
  public void testValidateMissingArgumentColumns() {
    BigQueryArgumentSetterConfig config = getBuilder().setArgumentsColumns(null).build();
    validateConfigValidationFail(config, BigQueryArgumentSetterConfig.NAME_ARGUMENTS_COLUMNS);
  }

  @Test
  public void testCheckIfArgumentsColumnsListExistsInSourceMatchOne() {
    List<String> argumentsColumnsList = ImmutableList.of("name");
    FieldList fields = FieldList.of(Field.newBuilder("name", LegacySQLTypeName.STRING).build());
    BigQueryArgumentSetterConfig.checkIfArgumentsColumnsListExistsInSource(argumentsColumnsList, fields,
        mockFailureCollector);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testCheckIfArgumentsColumnsListExistsInSourceMatchSome() {
    List<String> argumentsColumnsList = ImmutableList.of("name");
    FieldList fields = FieldList.of(Field.newBuilder("name", LegacySQLTypeName.STRING).build(),
        Field.newBuilder("age", LegacySQLTypeName.INTEGER).build());
    BigQueryArgumentSetterConfig.checkIfArgumentsColumnsListExistsInSource(argumentsColumnsList, fields,
        mockFailureCollector);
    Assert.assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void testCheckIfArgumentsColumnsListExistsInSourceFailedMatchOne() {
    List<String> argumentsColumnsList = ImmutableList.of("name");
    FieldList fields = FieldList.of(Field.newBuilder("age", LegacySQLTypeName.INTEGER).build());
    BigQueryArgumentSetterConfig.checkIfArgumentsColumnsListExistsInSource(argumentsColumnsList, fields,
        mockFailureCollector);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
    Assert.assertEquals("Column: \"name\" does not exist in table. Argument columns must exist in table.",
        mockFailureCollector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testCheckIfArgumentsColumnsListExistsInSourceFailedMatchSome() {
    List<String> argumentsColumnsList = ImmutableList.of("name", "city");
    FieldList fields = FieldList.of(Field.newBuilder("age", LegacySQLTypeName.INTEGER).build());
    BigQueryArgumentSetterConfig.checkIfArgumentsColumnsListExistsInSource(argumentsColumnsList, fields,
        mockFailureCollector);
    Assert.assertEquals(1, mockFailureCollector.getValidationFailures().size());
    Assert.assertEquals("Columns: \"name ,city\" do not exist in table. Argument columns must exist in table.",
        mockFailureCollector.getValidationFailures().get(0).getMessage());
  }

  private static BigQueryArgumentSetterConfig.Builder getBuilder() {
    return BigQueryArgumentSetterConfig.builder()
      .setDatasetProject(VALID_DATASET_PROJECT)
      .setDataset(VALID_DATASET)
      .setTable(VALID_TABLE)
      .setArgumentSelectionConditions(VALID_ARGUMENT_SELECTION_CONDITIONS)
      .setArgumentsColumns(VALID_ARGUMENT_COLUMN);
  }

  private static void validateConfigValidationFail(BigQueryArgumentSetterConfig config, String causeAttribute) {
    MockFailureCollector collector = new MockFailureCollector();
    ValidationFailure failure;
    try {
      config.validate(collector);
      Assert.assertEquals(1, collector.getValidationFailures().size());
      failure = collector.getValidationFailures().get(0);
    } catch (ValidationException e) {
      Assert.assertEquals(1, e.getFailures().size());
      failure = e.getFailures().get(0);
    }
    Assert.assertEquals(causeAttribute, failure.getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }
}
