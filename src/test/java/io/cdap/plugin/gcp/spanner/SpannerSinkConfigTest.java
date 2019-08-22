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
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.spanner.sink.SpannerSinkConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class SpannerSinkConfigTest {

  @Test
  public void testInvalidPrimarykeys() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("dt", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("bytedata", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("timestamp",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));

    SpannerSinkConfig config = new SpannerSinkConfig("r", null, -1, null, null, "zip, name", schema.toString());
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector);

    Assert.assertEquals(2, collector.getValidationFailures().size());

    // first failure will be related to negative batchsize
    ValidationFailure failure = collector.getValidationFailures().get(0);
    List<ValidationFailure.Cause> causes = failure.getCauses();
    Assert.assertEquals(1, causes.size());
    Assert.assertEquals("batchSize", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));

    // second failure will be related to config element not present in the output schema
    failure = collector.getValidationFailures().get(1);
    causes = failure.getCauses();
    Assert.assertEquals("keys", causes.get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
    Assert.assertEquals("zip", causes.get(0).getAttribute(CauseAttributes.CONFIG_ELEMENT));
  }

  @Test
  public void testValidPrimarykeys() {
    Schema schema = Schema.recordOf("record",
                                    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
                                    Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                                    Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE)),
                                    Schema.Field.of("dt", Schema.nullableOf(Schema.of(Schema.LogicalType.DATE))),
                                    Schema.Field.of("bytedata", Schema.of(Schema.Type.BYTES)),
                                    Schema.Field.of("timestamp",
                                                    Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))));

    SpannerSinkConfig config = new SpannerSinkConfig("r", null, null, null, null, "id, name", schema.toString());
    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }
}
