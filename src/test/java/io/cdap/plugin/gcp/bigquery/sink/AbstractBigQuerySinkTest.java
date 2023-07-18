/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.sink;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link AbstractBigQuerySink}.
 */
public class AbstractBigQuerySinkTest {
  MockFailureCollector collector;
  Schema notNestedSchema;
  Schema nestedSchema;
  Schema oneLevelNestedSchema;

  @Before
  public void setup() throws NoSuchMethodException {
    collector = new MockFailureCollector();
    notNestedSchema = Schema.recordOf("test",
            Schema.Field.of("id", Schema.of(Schema.Type.INT)),
            Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("age", Schema.of(Schema.Type.INT)),
            Schema.Field.of("objectJson", Schema.of(Schema.Type.STRING)));
    nestedSchema = Schema.recordOf("nestedObject",
            Schema.Field.of("nestedId", Schema.of(Schema.Type.INT)),
            Schema.Field.of("nestedName", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("nestedAge", Schema.of(Schema.Type.INT)),
            Schema.Field.of("nestedObjectJson", Schema.of(Schema.Type.STRING)));
    oneLevelNestedSchema = Schema.recordOf("test",
            Schema.Field.of("id", Schema.of(Schema.Type.INT)),
            Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
            Schema.Field.of("age", Schema.of(Schema.Type.INT)),
            Schema.Field.of("nested", nestedSchema));
  }

  @Test
  public void testValidateJsonStringFieldsNoNesting() {
    String jsonFields = "objectJson";
    new BigQuerySink(null).validateJsonStringFields(notNestedSchema, jsonFields, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testValidateJsonStringFieldsOneLevelNesting() {
    String jsonFields = "nested.nestedObjectJson";
    new BigQuerySink(null).validateJsonStringFields(oneLevelNestedSchema, jsonFields, collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());

  }

  @Test
  public void testValidateJsonStringFieldsOneLevelNestingNotString() {

    String jsonFields = "nested.nestedId";
    new BigQuerySink(null).validateJsonStringFields(oneLevelNestedSchema, jsonFields, collector);
    Assert.assertEquals(String.format(
                    "Field '%s' is of type '%s' which is not supported for conversion to JSON string.",
                    "nested.nestedId", Schema.Type.INT),
            collector.getValidationFailures().stream().findFirst().get().getMessage());

  }

  @Test
  public void testValidateJsonStringFieldsDoesNotExist() {
    String jsonFields = "nested.nestedObjectJson";
    new BigQuerySink(null).validateJsonStringFields(notNestedSchema, jsonFields, collector);
    Assert.assertEquals(String.format("Field(s) '%s' are not present in the Output Schema.", "nested.nestedObjectJson"),
            collector.getValidationFailures().stream().findFirst().get().getMessage());
  }

}
