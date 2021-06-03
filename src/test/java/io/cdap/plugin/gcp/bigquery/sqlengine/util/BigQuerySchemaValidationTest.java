/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.sqlengine.util;

import io.cdap.cdap.api.data.schema.Schema;
import org.junit.Assert;
import org.junit.Test;

public class BigQuerySchemaValidationTest {
  @Test
  public void testSupportsSchema() {
    Schema schemaWithEnum = Schema.recordOf("Users",
                                            Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                            Schema.Field.of("enum", Schema.enumWith("a", "b")));
    BigQuerySchemaValidation schemaWithEnumValidation = BigQuerySchemaValidation.validateSchema(schemaWithEnum);
    Assert.assertFalse(schemaWithEnumValidation.isSupported());
    Assert.assertEquals(1, schemaWithEnumValidation.getInvalidFields().size());
    Assert.assertEquals("enum", schemaWithEnumValidation.getInvalidFields().get(0));

    Schema schemaWithMap = Schema.recordOf("Users",
                                           Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                           Schema.Field.of("map",
                                                           Schema.mapOf(Schema.of(Schema.Type.INT),
                                                                        Schema.of(Schema.Type.INT))));
    BigQuerySchemaValidation schemaWithMapValidation = BigQuerySchemaValidation.validateSchema(schemaWithMap);
    Assert.assertFalse(schemaWithMapValidation.isSupported());
    Assert.assertEquals(1, schemaWithMapValidation.getInvalidFields().size());
    Assert.assertEquals("map", schemaWithMapValidation.getInvalidFields().get(0));

    Schema schemaWithUnion = Schema.recordOf("Users",
                                             Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                             Schema.Field.of("union",
                                                             Schema.unionOf(Schema.of(Schema.Type.INT),
                                                                            Schema.of(Schema.Type.LONG))));
    BigQuerySchemaValidation schemaWithUnionValidation = BigQuerySchemaValidation.validateSchema(schemaWithUnion);
    Assert.assertFalse(schemaWithUnionValidation.isSupported());
    Assert.assertEquals(1, schemaWithUnionValidation.getInvalidFields().size());
    Assert.assertEquals("union", schemaWithUnionValidation.getInvalidFields().get(0));

    Schema schemaWithValidFields = Schema.recordOf("Users",
                                                   Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                                   Schema.Field.of("null", Schema.of(Schema.Type.NULL)),
                                                   Schema.Field.of("boolean", Schema.of(Schema.Type.BOOLEAN)),
                                                   Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
                                                   Schema.Field.of("float", Schema.of(Schema.Type.FLOAT)),
                                                   Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
                                                   Schema.Field.of("bytes", Schema.of(Schema.Type.BYTES)),
                                                   Schema.Field.of("string", Schema.of(Schema.Type.STRING)),
                                                   Schema.Field.of("array",
                                                                   Schema.arrayOf(Schema.of(Schema.Type.STRING))),
                                                   Schema.Field.of("record",
                                                                   Schema.recordOf("some_record")));
    BigQuerySchemaValidation schemaWithValidFieldsValidation =
      BigQuerySchemaValidation.validateSchema(schemaWithValidFields);
    Assert.assertTrue(schemaWithValidFieldsValidation.isSupported());
    Assert.assertEquals(0, schemaWithValidFieldsValidation.getInvalidFields().size());
  }
}
