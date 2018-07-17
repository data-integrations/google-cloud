/*
 * Copyright © 2018 Google Inc.
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

package co.cask.spanner;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.ReferenceBatchSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


/**
 * This class implements a {@link ReferenceBatchSink} to write to Google Spanner.
 *
 * Uses a {@link SpannerOutputFormat} and {@link SpannerRecordWriter} to write configure
 * and write to spanner. The <code>prepareRun</code> method configures the job by extracting
 * the user provided configuration and preparing it to be passed to {@link SpannerOutputFormat}.
 *
 * @see SpannerOutputFormat
 * @see SpannerRecordWriter
 */
@Plugin(type = "batchsink")
@Name(SpannerSink.NAME)
@Description(SpannerSink.DESCRIPTION)
public final class SpannerSink
  extends ReferenceBatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  public static final String NAME = "Spanner";
  public static final String DESCRIPTION = "Spanner Sink";

  private Configuration configuration;
  private final SpannerSinkConfig config;

  /**
   * Initializes <code>SpannerSink</code>.
   * @param config
   */
  public SpannerSink(SpannerSinkConfig config) {
    super(config);
    this.config = config;
  }


  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    Job job = Job.getInstance();

    // some input formats require the credentials to be present in the job. We don't know for
    // sure which ones (HCatalog is one of them), so we simply always add them. This has no other
    // effect, because this method is only used at configure time and will be ignored later on.
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      job.getCredentials().addAll(credentials);
    }

    configuration = job.getConfiguration();
    configuration.clear();

    configuration.set(SpannerConstants.PROJECT_ID, config.project);
    configuration.set(SpannerConstants.SERVICE_ACCOUNT_FILE_PATH, config.serviceAccountFilePath);
    configuration.set(SpannerConstants.TABLE_NAME, config.table);
    configuration.set(SpannerConstants.INSTANCE_ID, config.instanceid);
    configuration.set(SpannerConstants.DATABASE, config.database);

    context.addOutput(Output.of(config.table.replace("-", "_").replace(".", "_"),
                                new OutputFormatProvider() {
      @Override
      public String getOutputFormatClassName() {
        return SpannerOutputFormat.class.getName();
      }

      @Override
      public Map<String, String> getOutputFormatConfiguration() {
        Map<String, String> config = new HashMap<>();
        for (Map.Entry<String, String> entry : configuration) {
          config.put(entry.getKey(), entry.getValue());
        }
        return config;
      }
    }));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    try {
      Schema.parseJson(config.schema);
    } catch (IOException e) {
      throw new IllegalArgumentException(
        String.format("Unable to parse output schema. Reason: %s", e.getMessage()), e
      );
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable,
    StructuredRecord>> emitter)
    throws Exception {
    emitter.emit(new KeyValue<NullWritable, StructuredRecord>(null, input));
  }

  @Override
  public void destroy() {
    super.destroy();
  }
}
