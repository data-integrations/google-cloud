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

package co.cask.gcp.common;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link AvroToStructuredTransformer}
 */
public class MultiStructuredToParquetTransformerTest {

    @Test
    public void testAvroToStructuredRecordUnsupportedType() throws Exception {

        Schema schema = Schema.recordOf(
                "MULTI3",
                Schema.Field.of("ITEM", Schema.of(Schema.Type.STRING)),
                Schema.Field.of("CODE", Schema.nullableOf(Schema.of(Schema.Type.INT))));


        StructuredRecord record = StructuredRecord.builder(schema)
                .set("ITEM", "donut")
                .set("CODE", 100).build();

        MultiStructuredToParquetTransformer avroToStructuredTransformer = new MultiStructuredToParquetTransformer(schema);

        GenericRecord result = avroToStructuredTransformer.transform(record, schema);
        assertTrue(true);
    }
}
