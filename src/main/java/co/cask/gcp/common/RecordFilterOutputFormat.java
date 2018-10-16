/*
 * Copyright © 2017 Cask Data, Inc.
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
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.orc.mapreduce.OrcOutputFormat;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;

/**
 * An OutputFormat that filters records before sending them to a delegate. Currently only supports TextOutputFormat
 * as the delegate, but we may want to add other formats in the future.
 */
public class RecordFilterOutputFormat extends OutputFormat<NullWritable, StructuredRecord> {
    public static final String FILTER_FIELD = "record.filter.field";
    public static final String PASS_VALUE = "record.filter.val";
    public static final String DELIMITER = "record.filter.delimiter";
    public static final String FORMAT = "record.output.format";
    public static final String ORIGINAL_SCHEMA = "record.original.schema";
    public static final String AVRO = "avro";
    public static final String TEXT = "text";
    public static final String ORC = "orc";
    public static final String PARQUET = "parquet";


    @Override
    public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        String filterField = conf.get(FILTER_FIELD);
        String passthroughVal = conf.get(PASS_VALUE);
        String delimiter = conf.get(DELIMITER);
        String format = conf.get(FORMAT);
        String originalSchema = conf.get(ORIGINAL_SCHEMA);

        return new FilterRecordWriter(getOutputFormat(format).getRecordWriter(context),
                filterField, passthroughVal, getStructuredRecordTransformer(format, delimiter),
                originalSchema);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        getOutputFormat(context.getConfiguration().get(FORMAT)).checkOutputSpecs(context);
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return getOutputFormat(context.getConfiguration().get(FORMAT)).getOutputCommitter(context);
    }

    private OutputFormat getOutputFormat(String format) {
        if (AVRO.equals(format)) {
            return new AvroKeyOutputFormat<>();
        } else if (ORC.equals(format)) {
            return new OrcOutputFormat<>();
        } else if (PARQUET.equals(format)) {
            return new AvroParquetOutputFormat();
        } else {
            return new TextOutputFormat<>();
        }
    }

    private AbstractStructuredRecordTransformer getStructuredRecordTransformer(String format, String delimiter) {
        if (AVRO.equals(format)) {
            return new MultiStructuredToAvroTransformer(null);
        } else if (ORC.equals(format)) {
            return new StructuredToOrcTransformer(null);
        } else if (PARQUET.equals(format)) {
            return new MultiStructuredToParquetTransformer(null);
        } else {
            return new StructuredToTextTransformer(delimiter, null);
        }
    }

    /**
     * Filters records before writing them out as text.
     */
    public static class FilterRecordWriter extends RecordWriter<NullWritable, StructuredRecord> {
        private final String filterField;
        private final String passthroughValue;
        private final RecordWriter delegate;
        private final AbstractStructuredRecordTransformer transformer;
        private final Schema schema;

        FilterRecordWriter(RecordWriter delegate, String filterField, String passthroughValue,
                           AbstractStructuredRecordTransformer transformer, String originalSchema) throws IOException {
            this.filterField = filterField;
            this.passthroughValue = passthroughValue;
            this.delegate = delegate;
            this.transformer = transformer;
            this.schema = Schema.parseJson(originalSchema);
        }

        @Override
        public void write(NullWritable key, StructuredRecord record) throws IOException, InterruptedException {
            String val = record.get(filterField);
            if (passthroughValue.equals(val)) {
                StructuredRecord.Builder recordBuilder = StructuredRecord.builder(schema);
                for (Schema.Field field : record.getSchema().getFields()) {
                    String fieldName = field.getName();
                    if (filterField.equals(fieldName)) {
                        continue;
                    }
                    Object fieldVal = record.get(fieldName);
                    recordBuilder.set(fieldName, fieldVal);
                }
                if (transformer instanceof MultiStructuredToAvroTransformer) {
                    delegate.write(new AvroKey<>(transformer.transform(recordBuilder.build())), key);
                } else if (transformer instanceof MultiStructuredToParquetTransformer){
                    delegate.write(null, transformer.transform(recordBuilder.build()));
                } else {
                    delegate.write(key, transformer.transform(recordBuilder.build()));
                }
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            delegate.close(context);
        }
    }
}
