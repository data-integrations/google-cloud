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

package co.cask.gcs.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.api.streaming.StreamingSource;
import org.apache.spark.streaming.api.java.JavaDStream;
import scala.reflect.ClassTag$;

import java.io.Serializable;

/**
 * Realtime source plugin to read from Google PubSub.
 */
@Plugin(type = StreamingSource.PLUGIN_TYPE)
@Name("GoogleSubscriber")
@Description("Streaming Source to read events from Google PubSub.")
public class GoogleSubscriber extends StreamingSource<StructuredRecord> {
	private static final Schema DEFAULT_SCHEMA = Schema.recordOf("event",
			Schema.Field.of("message",Schema.of(Schema.Type.STRING)),
			Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
			Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG)));

	private SubscriberConfig config;

	@Override
	public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
		pipelineConfigurer.getStageConfigurer().setOutputSchema(DEFAULT_SCHEMA);
	}

	@Override
	public JavaDStream<StructuredRecord> getStream(StreamingContext streamingContext) throws Exception {
		PubSubDStream dStream = new PubSubDStream(streamingContext.getSparkStreamingContext().ssc(), config.projectId,
				config.subscriberId, config.serviceFilePath, 10);
		return JavaDStream.fromDStream(dStream, ClassTag$.MODULE$.<StructuredRecord>apply(StructuredRecord.class));
	}


	public static class SubscriberConfig extends PluginConfig implements Serializable {
		@Name("project")
		@Description("Project Id")
		@Macro
		private String projectId;

		@Name("subscriber")
		@Description("Subscriber ID associated with the topic being subscribed to")
		@Macro
		private String subscriberId;

		@Name("serviceFilePath")
		@Description("Service File Path")
		@Macro
		private String serviceFilePath;
	}
}
