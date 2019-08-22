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

package io.cdap.plugin.gcp.bigquery;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySink;
import io.cdap.plugin.gcp.bigquery.sink.BigQuerySinkConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@link BigQuerySink}.
 */
public class BigQuerySinkTest {

  @Test
  public void testBigQuerySinkConfig() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("dt", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("bytedata", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("timestamp",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));

    BigQuerySinkConfig config = new BigQuerySinkConfig("44", "ds", "tb", "bucket", schema.toString());
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testBigQuerySinkInvalidConfig() {
    Schema invalidSchema = Schema.recordOf("record",
                                           Schema.Field.of("id", Schema.of(Schema.Type.LONG)));

    BigQuerySinkConfig config = new BigQuerySinkConfig("reference!!", "ds", "tb", "buck3t$$", invalidSchema.toString());
    MockFailureCollector collector = new MockFailureCollector("bqsink");
    config.validate(collector);
    List<ValidationFailure> failures = collector.getValidationFailures();
    Assert.assertEquals(2, failures.size());
  }
}
