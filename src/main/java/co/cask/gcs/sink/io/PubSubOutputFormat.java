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

package co.cask.gcs.sink.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * OutputFormat to write to Pubsub topic.
 */
public class PubSubOutputFormat extends OutputFormat<NullWritable, Text> {


	@Override
	public RecordWriter<NullWritable, Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException,
			InterruptedException {
		Configuration config = taskAttemptContext.getConfiguration();
		return new PubSubRecordWriter(config.get("pubsub.project"), config.get("pubsub.topic"),
				config.get("pubsub.serviceFilePath"));
	}

	@Override
	public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		return new OutputCommitter() {

			@Override
			public void setupJob(JobContext jobContext) throws IOException {

			}

			@Override
			public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

			}

			@Override
			public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
				return false;
			}

			@Override
			public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

			}

			@Override
			public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

			}
		};
	}
}
