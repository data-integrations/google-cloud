/*
 * Copyright © 2018 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.plugin.gcp.spanner;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.spanner.common.SpannerUtil;
import org.junit.Assert;
import org.junit.Test;

public class SpannerUtilTest {

  @Test
  public void convertSchemaTest() {
    Schema schema = Schema.recordOf(
      "record",

      Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("dt", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
      Schema.Field.of("bytedata", Schema.of(Schema.Type.BYTES)),
      Schema.Field.of("timestamp",
                      Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))),
      Schema.Field.of("int_array", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.LONG)))),
      Schema.Field.of("string_array", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.STRING)))),
      Schema.Field.of("bool_array", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)))),
      Schema.Field.of("float_array", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)))),
      Schema.Field.of("bytes_array", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.Type.BYTES)))),
      Schema.Field.of("timestamp_array",
                      Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)))),
      Schema.Field.of("date_array", Schema.arrayOf(Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))))

    );

    Assert.assertEquals("CREATE TABLE table (id INT64 NOT NULL, name STRING(MAX) NOT NULL, " +
                          "price FLOAT64 NOT NULL, dt DATE, bytedata BYTES(MAX) NOT NULL, timestamp TIMESTAMP, " +
                          "int_array ARRAY<INT64> NOT NULL, string_array ARRAY<STRING(MAX)> NOT NULL, " +
                          "bool_array ARRAY<BOOL> NOT NULL, float_array ARRAY<FLOAT64> NOT NULL, " +
                          "bytes_array ARRAY<BYTES(MAX)> NOT NULL, timestamp_array ARRAY<TIMESTAMP> NOT NULL, " +
                          "date_array ARRAY<DATE> NOT NULL) PRIMARY KEY (id, name)",
                        SpannerUtil.convertSchemaToCreateStatement("table", "id, name", schema));
  }
}
