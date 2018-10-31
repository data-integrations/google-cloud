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

package co.cask.gcp.bigquery;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.format.UnexpectedFormatException;
import co.cask.cdap.api.data.schema.Schema;
import com.google.common.collect.ImmutableList;
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

  @Test(expected = UnexpectedFormatException.class)
  public void testAvroToStructuredRecordUnsupportedType() throws Exception {

    BigQueryAvroToStructuredTransformer avroToStructuredTransformer = new BigQueryAvroToStructuredTransformer();

    Schema innerNestedSchema = Schema.recordOf("innerNested",
                                               Schema.Field.of("int", Schema.of(Schema.Type.INT)));
    Schema innerSchema = Schema.recordOf(
      "inner",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("double", Schema.of(Schema.Type.DOUBLE)),
      Schema.Field.of("array", Schema.arrayOf(Schema.of(Schema.Type.FLOAT)))
    );

    Schema schema = Schema.recordOf(
      "record",
      Schema.Field.of("int", Schema.of(Schema.Type.INT)),
      Schema.Field.of("record", innerSchema));

    org.apache.avro.Schema avroInnerSchema = convertSchema(innerSchema);
    org.apache.avro.Schema avroSchema = convertSchema(schema);
    org.apache.avro.Schema avroInnerNestedSchema = convertSchema(innerNestedSchema);

    GenericRecord inner = new GenericRecordBuilder(avroInnerNestedSchema)
      .set("int", 0)
      .build();
    GenericRecord record = new GenericRecordBuilder(avroSchema)
      .set("int", Integer.MAX_VALUE)
      .set("record",
           new GenericRecordBuilder(avroInnerSchema)
             .set("int", 5)
             .set("double", 3.14159)
             .set("array", ImmutableList.of(1.0f, 2.0f))
             .build())
      .build();

    StructuredRecord result = avroToStructuredTransformer.transform(record, schema);
  }

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
