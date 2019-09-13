/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.format.UnexpectedFormatException;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.plugin.gcp.bigquery.source.BigQueryAvroToStructuredTransformer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalTime;

/**
 * Tests for {@link BigQueryAvroToStructuredTransformer}
 */
public class AvroToStructuredRecordTest {

  @Test
  public void testAvroToStructuredRecord() throws Exception {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("dt", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("time", Schema.of(Schema.LogicalType.TIME_MICROS)),
                                    Schema.Field.of("timestamp",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));

    org.apache.avro.Schema avroSchema = convertSchema(schema);
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("id", 1L)
      .set("name", "alice")
      .set("price", 1.2d)
      .set("dt", "2018-11-11")
      .set("time", "11:11:11")
      .set("timestamp", 1464181635000000L)
      .build();

    BigQueryAvroToStructuredTransformer transformer = new BigQueryAvroToStructuredTransformer();
    StructuredRecord actual = transformer.transform(record, schema);

    StructuredRecord expected = StructuredRecord.builder(schema)
      .set("id", 1L)
      .set("name", "alice")
      .set("price", 1.2d)
      .setDate("dt", LocalDate.of(2018, 11, 11))
      .setTime("time", LocalTime.of(11, 11, 11))
      .set("timestamp", 1464181635000000L).build();

    Assert.assertEquals(expected, actual);

    record = new GenericRecordBuilder(avroSchema)
      .set("id", 1L)
      .set("name", "alice")
      .set("price", 1.2d)
      .set("dt", "2018-11-11")
      .set("time", "11:11:11")
      .set("timestamp", null)
      .build();

    actual = transformer.transform(record, schema);
    expected = StructuredRecord.builder(schema)
      .set("id", 1L)
      .set("name", "alice")
      .set("price", 1.2d)
      .setDate("dt", LocalDate.of(2018, 11, 11))
      .setTime("time", LocalTime.of(11, 11, 11))
      .set("timestamp", null).build();

    Assert.assertEquals(expected, actual);

    Schema innerSchema = Schema.recordOf(
      "inner",
      Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.DOUBLE)))
    );

    schema = Schema.recordOf(
      "record",
      Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("record", innerSchema));

    org.apache.avro.Schema avroInnerSchema = convertSchema(innerSchema);
    avroSchema = convertSchema(schema);
    record = new GenericRecordBuilder(avroSchema)
      .set("long", Long.MAX_VALUE)
      .set("record",
           new GenericRecordBuilder(avroInnerSchema)
             .set("long", 5L)
             .set("double", 3.14159)
             .set("array", ImmutableList.of(1.0f, 2.0f))
             .build())
      .build();

    actual = transformer.transform(record, schema);
    expected = StructuredRecord.builder(schema)
      .set("long", Long.MAX_VALUE)
      .set("record", StructuredRecord.builder(innerSchema)
        .set("long", 5L)
        .set("double", 3.14159)
        .set("array", ImmutableList.of(1.0f, 2.0f))
        .build())
      .build();

    Assert.assertEquals(expected, actual);

    Schema innerNestedSchema = Schema.recordOf(
      "innerNested",
      Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("string", Schema.of(Schema.Type.STRING))
    );
    innerSchema = Schema.recordOf(
      "inner",
      Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.DOUBLE))),
      Schema.Field.of("innerRecord", innerNestedSchema)
    );
    schema = Schema.recordOf(
      "record",
      Schema.Field.of("long", Schema.of(Schema.Type.LONG)),
      Schema.Field.of("record", innerSchema));

    org.apache.avro.Schema avroInnerNestedSchema = convertSchema(innerNestedSchema);
    avroInnerSchema = convertSchema(innerSchema);
    avroSchema = convertSchema(schema);
    record = new GenericRecordBuilder(avroSchema)
      .set("long", Long.MAX_VALUE)
      .set("record",
           new GenericRecordBuilder(avroInnerSchema)
             .set("long", 5L)
             .set("double", 3.14159)
             .set("array", ImmutableList.of(1.0f, 2.0f))
             .set("innerRecord",
                  new GenericRecordBuilder(avroInnerNestedSchema)
                    .set("long", 10L)
                    .set("string", "test")
                    .build())
             .build())
      .build();

    actual = transformer.transform(record, schema);
    expected = StructuredRecord.builder(schema)
      .set("long", Long.MAX_VALUE)
      .set("record", StructuredRecord.builder(innerSchema)
        .set("long", 5L)
        .set("double", 3.14159)
        .set("array", ImmutableList.of(1.0f, 2.0f))
        .set("innerRecord", StructuredRecord.builder(innerNestedSchema)
          .set("long", 10L)
          .set("string", "test")
          .build())
        .build())
      .build();

    Assert.assertEquals(expected, actual);
  }

  @Test(expected = UnexpectedFormatException.class)
  public void testUnionTypeUnsupported() throws Exception {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("union",
                                                    Schema.unionOf(Schema.of(Schema.Type.STRING),
                                                                   Schema.of(Schema.Type.INT))));
    BigQueryAvroToStructuredTransformer transformer = new BigQueryAvroToStructuredTransformer();
    org.apache.avro.Schema avroSchema = convertSchema(schema);
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("id", 1L)
      .set("name", "alice")
      .set("price", 1.2d)
      .set("union", "string")
      .build();

    StructuredRecord actual = transformer.transform(record, schema);
  }

  private org.apache.avro.Schema convertSchema(Schema cdapSchema) {
    return new org.apache.avro.Schema.Parser().parse(cdapSchema.toString());
  }
}
