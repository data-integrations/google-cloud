/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigtable.source;

import com.google.bigtable.repackaged.com.google.cloud.ServiceOptions;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.common.Constants;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

public class BigtableSourceConfigTest {
  private static final String VALID_REF = "test-ref";
  private static final String VALID_TABLE = "test-table";
  private static final String VALID_INSTANCE = "test-instance";
  private static final String VALID_COLUMN_MAPPING = "test-family:id=id";
  private static final String VALID_BIGTABLE_OPTIONS = "";
  private static final String VALID_PROJECT = "test-project";
  private static final String VALID_ACCOUNT_FILE_PATH =
    BigtableSourceConfigTest.class.getResource("/credentials.json").getPath();
  private static final String VALID_KEY_ALIAS = "test-alias";
  private static final String VALID_SCAN_ROW_START = "test-scan-row-start";
  private static final String VALID_SCAN_ROW_STOP = "test-scan-row-stop";
  private static final Long VALID_SCAN_TIME_RANGE_START = 0L;
  private static final Long VALID_SCAN_TIME_RANGE_STOP = 1L;
  private static final String VALID_ON_ERROR = "fail-pipeline";
  private static final String VALID_SCHEMA =
    Schema.recordOf("record", Schema.Field.of("id", Schema.of(Schema.Type.LONG))).toString();

  @Test
  public void testValidateValidConfig() {
    BigtableSourceConfig config = getBuilder()
      .build();

    config.validate(null);
  }

  @Test
  @Ignore
  public void testValidateReference() {
    BigtableSourceConfig config = getBuilder()
      .setReferenceName("")
      .build();

    // TODO: (vinisha) validate failure instead of stage config once this method is migrated to new api
    validateConfigValidationFail(config, Constants.Reference.REFERENCE_NAME);
  }

  @Test
  public void testValidateMissingTable() {
    BigtableSourceConfig config = getBuilder()
      .setTable(null)
      .build();

    validateConfigValidationFail(config, BigtableSourceConfig.TABLE);
  }

  @Test
  public void testValidateMissingInstanceId() {
    BigtableSourceConfig config = getBuilder()
      .setInstance(null)
      .build();

    validateConfigValidationFail(config, BigtableSourceConfig.INSTANCE);
  }

  @Test
  public void testValidateMissingProjectId() {
    Assume.assumeTrue(ServiceOptions.getDefaultProjectId() == null);

    BigtableSourceConfig config = getBuilder()
      .setProject(null)
      .build();

    validateConfigValidationFail(config, BigtableSourceConfig.PROJECT);
  }

  @Test
  public void testValidateMissingCredentialsFile() {
    BigtableSourceConfig config = getBuilder()
      .setServiceFilePath("/tmp/non_existing_file")
      .build();

    validateConfigValidationFail(config, BigtableSourceConfig.SERVICE_ACCOUNT_FILE_PATH);
  }

  @Test
  public void testValidateMissingErrorHandling() {
    BigtableSourceConfig config = getBuilder()
      .setOnError("")
      .build();

    validateConfigValidationFail(config, BigtableSourceConfig.ON_ERROR);
  }

  @Test
  public void testValidateMissingSchema() {
    BigtableSourceConfig config = getBuilder()
      .setSchema(null)
      .build();

    validateConfigValidationFail(config, BigtableSourceConfig.SCHEMA);
  }

  @Test
  public void testValidateInvalidSchema() {
    BigtableSourceConfig config = getBuilder()
      .setSchema("")
      .build();

    validateConfigValidationFail(config, BigtableSourceConfig.SCHEMA);
  }

  @Test
  public void testValidateInvalidFieldNameInSchema() {
    BigtableSourceConfig config = getBuilder()
      .setSchema(Schema.recordOf("record", Schema.Field.of("my_id", Schema.of(Schema.Type.LONG))).toString())
      .build();

    validateConfigValidationFail(config, BigtableSourceConfig.COLUMN_MAPPINGS);
  }

  @Test
  public void testValidateNoColumnInMapping() {
    BigtableSourceConfig config = getBuilder()
      .setColumnMappings("test-family:id=my_id") // no mapping for 'id' column
      .build();

    validateConfigValidationFail(config, BigtableSourceConfig.COLUMN_MAPPINGS);
  }

  @Test
  public void testValidateNoMappedColumnsInSchema() {
    BigtableSourceConfig config = getBuilder()
      .setSchema(
        Schema.recordOf(
          "record",
          Schema.Field.of("id", Schema.of(Schema.Type.INT)),
          Schema.Field.of("age", Schema.of(Schema.Type.INT))
        ).toString())
      .build();

    validateConfigValidationFail(config, BigtableSourceConfig.COLUMN_MAPPINGS);
  }

  private static BigtableSourceConfigBuilder getBuilder() {
    return BigtableSourceConfigBuilder.aBigtableSourceConfig()
      .setReferenceName(VALID_REF)
      .setTable(VALID_TABLE)
      .setInstance(VALID_INSTANCE)
      .setProject(VALID_PROJECT)
      .setServiceFilePath(VALID_ACCOUNT_FILE_PATH)
      .setScanRowStart(VALID_SCAN_ROW_START)
      .setScanRowStop(VALID_SCAN_ROW_STOP)
      .setScanTimeRangeStart(VALID_SCAN_TIME_RANGE_START)
      .setScanTimeRangeStop(VALID_SCAN_TIME_RANGE_STOP)
      .setOnError(VALID_ON_ERROR)
      .setKeyAlias(VALID_KEY_ALIAS)
      .setSchema(VALID_SCHEMA)
      .setColumnMappings(VALID_COLUMN_MAPPING)
      .setBigtableOptions(VALID_BIGTABLE_OPTIONS);
  }

  private static void validateConfigValidationFail(BigtableSourceConfig config, String propertyValue) {
    try {
      config.validate(new MockFailureCollector("stage"));
      Assert.fail(String.format("Expected to throw %s", InvalidConfigPropertyException.class.getName()));
    } catch (InvalidConfigPropertyException e) {
      Assert.assertEquals(propertyValue, e.getProperty());
    }
  }
}
