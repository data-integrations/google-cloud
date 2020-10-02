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

import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

public class BigQueryArgumentSetterConfigTest {

  private static final String VALID_DATASET = "dataset";
  private static final String VALID_TABLE = "table";
  private static final String VALID_ARGUMENT_SELECTION_CONDITIONS = "feed=10;id=0";
  private static final String VALID_ARGUMENT_COLUMN = "name";

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

  private static BigQueryArgumentSetterConfigBuilder getBuilder() {
    return BigQueryArgumentSetterConfigBuilder.bigQueryArgumentSetterConfig()
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
