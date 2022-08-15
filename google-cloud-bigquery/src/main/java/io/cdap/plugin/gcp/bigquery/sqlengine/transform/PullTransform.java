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

package io.cdap.plugin.gcp.bigquery.sqlengine.transform;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.SerializableTransform;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.io.LongWritable;

/**
 * Transform used when pulling records from BigQuery.
 */
public class PullTransform extends SerializableTransform<KeyValue<LongWritable, GenericData.Record>, StructuredRecord> {
  private final SQLEngineAvroToStructuredTransformer transformer;
  private final Schema schema;

  public PullTransform(Schema schema) {
    this.transformer = new SQLEngineAvroToStructuredTransformer();
    this.schema = schema;
  }

  @Override
  public void transform(KeyValue<LongWritable, GenericData.Record> input, Emitter<StructuredRecord> emitter)
    throws Exception {
    StructuredRecord transformed = transformer.transform(input.getValue(), schema);
    emitter.emit(transformed);
  }
}
