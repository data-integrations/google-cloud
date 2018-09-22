/*
 *
 *  * Copyright © 2017 Cask Data, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  * use this file except in compliance with the License. You may obtain a copy of
 *  * the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  * License for the specific language governing permissions and limitations under
 *  * the License.
 *
 */

package co.cask.gcp.gcs.source;

import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.plugin.EndpointPluginContext;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.gcp.common.CombinePathTrackingInputFormat;
import co.cask.gcp.common.PathTrackingInputFormat;
import co.cask.hydrator.common.ReferenceBatchSource;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Abstract file batch source
 */
public abstract class AbstractFileBatchSource extends ReferenceBatchSource<Object, Object, StructuredRecord> {
  public static final String INPUT_NAME_CONFIG = "input.path.name";
  public static final String INPUT_REGEX_CONFIG = "input.path.regex";
  public static final Schema DEFAULT_SCHEMA = Schema.recordOf(
    "event",
    Schema.Field.of("offset", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("body", Schema.of(Schema.Type.STRING))
  );

  private static final Logger LOG = LoggerFactory.getLogger(AbstractFileBatchSource.class);
  private final FileSourceConfig config;
  private FileSystem fs;

  public AbstractFileBatchSource(FileSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    config.validate();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(DEFAULT_SCHEMA);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {

    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    Map<String, String> properties = config.getFileSystemProperties();
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }

    if (config.fileRegex != null) {
      conf.set(INPUT_REGEX_CONFIG, config.fileRegex);
    }

    FileInputFormat.setInputDirRecursive(job, config.isRecursive());

    FileSystem pathFileSystem = FileSystem.get(new Path(config.getPath()).toUri(), conf);
    FileStatus[] fileStatus = pathFileSystem.globStatus(new Path(config.getPath()));
    fs = FileSystem.get(conf);

    if (fileStatus == null && config.shouldIgnoreNonExistingFolders()) {
      Path path = fs.getWorkingDirectory().suffix("/tmp/tmp.txt");
      LOG.warn(String.format("File/Folder specified in %s does not exists. Setting input path to %s.", config.getPath(),
                             path));
      fs.createNewFile(path);
      conf.set(INPUT_NAME_CONFIG, path.toUri().getPath());
      FileInputFormat.addInputPath(job, path);
    } else {
      conf.set(INPUT_NAME_CONFIG, new Path(config.getPath()).toString());
      FileInputFormat.addInputPath(job, new Path(config.getPath()));
    }

    if (config.maxSplitSize != null) {
      FileInputFormat.setMaxInputSplitSize(job, config.maxSplitSize);
    }
    PathTrackingInputFormat.configure(conf, config.pathField, config.useFilenameOnly());
    context.setInput(Input.of(config.referenceName,
                              new SourceInputFormatProvider(CombinePathTrackingInputFormat.class, conf)));
  }

  @Override
  public void transform(KeyValue<Object, Object> input, Emitter<StructuredRecord> emitter) {
    emitter.emit((StructuredRecord) input.getValue());
  }

  class Request {
    public String pathField;
  }

  /**
   * Endpoint method to get the output schema of a query.
   *
   * @param request Config for the plugin.
   * @param pluginContext context to create plugins
   * @return output schema
   */
  @javax.ws.rs.Path("getSchema")
  public Schema getSchema(Request request, EndpointPluginContext pluginContext) {
    return PathTrackingInputFormat.getOutputSchema(request.pathField);
  }
}
