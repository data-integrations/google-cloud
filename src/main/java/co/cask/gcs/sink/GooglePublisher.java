/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.gcs.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.gcs.sink.io.PubSubOutputFormat;
import co.cask.hydrator.common.ReferenceBatchSink;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.collect.Maps;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.util.Map;

/**
 * Google Publisher sink to write to a Google PubSub topic.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("GooglePublisher")
@Description("Streaming Source to write events to Google PubSub.")
public class GooglePublisher extends ReferenceBatchSink<StructuredRecord, NullWritable, Text> {
  private Config publisherConfig;

  private final PubSubOutputFormatProvider pubsubOutputFormatProvider;

  @SuppressWarnings("unused")
  public GooglePublisher(Config config) {
    super(config);
    this.publisherConfig = config;
    pubsubOutputFormatProvider = new PubSubOutputFormatProvider(config);
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    context.addOutput(Output.of(publisherConfig.referenceName, pubsubOutputFormatProvider));
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable, Text>> emitter) throws Exception {
    String body = StructuredRecordStringConverter.toJsonString(input);
    emitter.emit(new KeyValue<>(NullWritable.get(), new Text(body)));
  }

  public static class Config extends ReferencePluginConfig {

    public Config(String referenceName, String projectId, String topic, String serviceFilePath) {
      super(referenceName);
      this.projectId = projectId;
      this.topic = topic;
      this.serviceFilePath = serviceFilePath;
    }

    @Name("project")
    @Description("Google Cloud Project Id")
    @Macro
    private String projectId;

    @Name("topic")
    @Description("Topic to publish the events")
    @Macro
    private String topic;

    @Name("serviceFilePath")
    @Description("Service account file path.")
    @Macro
    private String serviceFilePath;
  }

  private static class PubSubOutputFormatProvider implements OutputFormatProvider {
    private final Map<String, String> conf;

    public PubSubOutputFormatProvider(Config config) {
      this.conf = Maps.newHashMap();
      conf.put("pubsub.project", config.projectId);
      conf.put("pubsub.topic", config.topic);
      conf.put("pubsub.serviceFilePath", config.serviceFilePath);
    }

    @Override
    public String getOutputFormatClassName() {
      return PubSubOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }
}
