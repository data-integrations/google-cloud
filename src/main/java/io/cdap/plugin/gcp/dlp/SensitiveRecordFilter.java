/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.gcp.dlp;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.DlpServiceSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.privacy.dlp.v2.ByteContentItem;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.DeidentifyTemplate;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.Finding;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.GetInspectTemplateRequest;
import com.google.privacy.dlp.v2.InfoTypeTransformations;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.InspectResult;
import com.google.privacy.dlp.v2.InspectTemplate;
import com.google.privacy.dlp.v2.Likelihood;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.privacy.dlp.v2.RecordTransformations;
import com.google.protobuf.ByteString;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.MultiOutputEmitter;
import io.cdap.cdap.etl.api.MultiOutputPipelineConfigurer;
import io.cdap.cdap.etl.api.MultiOutputStageConfigurer;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class <code>SensitiveRecordFilter</code> provides an easy way to filter sensitive PII data from stream. The
 * class utilizes Data Loss Prevention APIs for identifying sensitive data. Depending on the filter confidence set by
 * user, the class either sends input record on sensitive port or non-sensitive port.
 *
 * <p>
 * In case of issue with invoking DLP, the plugin depending on user choice either chooses to skip record, error pipeline
 * or send record to error port.
 * </p>
 */
@Plugin(type = SplitterTransform.PLUGIN_TYPE)
@Name(SensitiveRecordFilter.NAME)
@Description(SensitiveRecordFilter.DESCRIPTION)
public final class SensitiveRecordFilter extends SplitterTransform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SensitiveRecordFilter.class);
  public static final String NAME = "SensitiveRecordFilter";
  public static final String DESCRIPTION = "Filters input records based that are sensitive.";
  private static final String SENSITIVE_PORT = "S";
  private static final String NON_SENSITIVE_PORT = "NS";

  // Stores the configuration passed to this class from user.
  private final Config config;

  // DLP service client for managing interactions with DLP service.
  private DlpServiceClient client;

  @VisibleForTesting
  public SensitiveRecordFilter(Config config) {
    this.config = config;
  }

  /**
   * Invoked during deployment of pipeline to validate configuration of the pipeline. This method checks if the input
   * specified is 'field' type and if it is, then checks if the field specified is present in the input schema.
   *
   * @throws IllegalArgumentException if there any issues with configuration of the plugin.
   * @param configurer a <code>MultiOutputPipelineConfigurer</code> for configuring pipeline.
   */
  @Override
  public void configurePipeline(MultiOutputPipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    LOG.error("got to configure");

    MultiOutputStageConfigurer stageConfigurer = configurer.getMultiOutputStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();

    if (client == null) {
      try {
        LOG.error(getSettings().toString());
        client = DlpServiceClient.create(getSettings());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    config.validate(stageConfigurer.getFailureCollector(), inputSchema, client);

    Map<String, Schema> outputs = new HashMap<>();
    outputs.put(SENSITIVE_PORT, inputSchema);
    outputs.put(NON_SENSITIVE_PORT, inputSchema);
    stageConfigurer.setOutputSchemas(outputs);
  }

  /**
   * Initialize this <code>SensitiveRecordFilter</code> plugin. A instance of DLP client is created with mapped
   * infotypes.
   *
   * @param context Initialization context
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    SensitiveDataMapping sensitivityMapping = new SensitiveDataMapping();
    List<InfoType> sensitiveInfoTypes = sensitivityMapping.getSensitiveInfoTypes(config.getSensitiveTypes());
    client = DlpServiceClient.create(getSettings());
  }

  /**
   * Splitter Transform splits the sensitive and non-sensitive record into different ports. If user has selected entire
   * record to be checked for sensitive data, then all the fields are concacted as string and passed  to data loss
   * prevention API.
   *
   * @param record a <code>StructuredRecord</code> being passed from the previous stage.
   * @param emitter a <code>MultiOutputEmitter</code> to emit sensitive or non-sensitive data on different ports.
   */
  @Override
  public void transform(StructuredRecord record, MultiOutputEmitter<StructuredRecord> emitter) throws Exception {
    String recordString = null;
    ContentItem contentItem = null;

    if (!config.isEntireRecord()) {
      recordString = record.get(config.getFieldName()).toString();
    } else {
      recordString = StructuredRecordStringConverter.toDelimitedString(record, ",");
    }
    try {

      // depending on input schema field object
      contentItem = ContentItem.newBuilder().setValue(recordString).build();

      String templateName = String.format("projects/%s/inspectTemplates/%s", config.getProject(), config.templateId);

      InspectContentRequest request =
        InspectContentRequest.newBuilder()
          .setParent(ProjectName.of(config.getProject()).toString())
          .setInspectTemplateName(templateName)
          .setItem(contentItem)
          .build();
      InspectContentResponse response = client.inspectContent(request);
      InspectResult result = response.getResult();

      if (result.getFindingsList().size() > 0) {
        emitter.emit(SENSITIVE_PORT, record);
        return;
      }

      emitter.emit(NON_SENSITIVE_PORT, record);
    } catch (Exception e) {
      switch (config.onErrorHandling()) {
        case -1:
          throw new Exception("Terminating pipeline on error as set in plugin configuration." + e.getMessage());
        case 0:
          return;
        case 1:
          emitter.emitError(new InvalidEntry<>(-1, e.getMessage(), record));
          break;
      }

    }
  }

  @Override
  public void destroy() {
    super.destroy();
    if (client != null) {
      client.close();
    }
  }


  /**
   * Configures the <code>DlpSettings</code> to use user specified service account file or auto-detect.
   *
   * @return Instance of <code>DlpServiceSettings</code>
   * @throws IOException thrown when there is issue reading service account file.
   */
  private DlpServiceSettings getSettings() throws IOException {
    DlpServiceSettings.Builder builder = DlpServiceSettings.newBuilder();
    if (config.getServiceAccountFilePath() != null) {
      builder.setCredentialsProvider(() -> GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath()));
    }
    return builder.build();
  }


  /**
   * Configuration object.
   */
  public static class Config extends GCPConfig {

    @Macro
    @Name("entire-record")
    @Description("Check full record or a field")
    private String entireRecord;

    @Macro
    @Name("field")
    @Description("Name of field to be inspected")
    @Nullable
    private String field;


    @Description("Information types to be matched")
    private String templateId;

    @Macro
    @Name("on-error")
    @Description("Error handling of record")
    private String onError;

    /**
     * @return The name of field that needs to be inspected for sensitive data.
     */
    public String getFieldName() {
      return field;
    }

    /**
     * @return true if entire record to checked for sensitive data.
     */
    public boolean isEntireRecord() {
      return entireRecord.equalsIgnoreCase("record");
    }

    /**
     * @return -1 to stop processing, 0 to skip record, 1 to emit record to error.
     */
    public int onErrorHandling() {
      if (onError.equalsIgnoreCase("stop-on-error")) {
        return -1;
      } else if (onError.equalsIgnoreCase("skip-record")) {
        return 0;
      } else {
        return 1;
      }
    }


    public void validate(FailureCollector collector, Schema inputSchema,
                         DlpServiceClient client) {
      if (!containsMacro("entire-record") && !isEntireRecord() && getFieldName() == null) {
        collector.addFailure("Input type is specified as 'Field', " +
                               "but a field name has not been specified.", "Specify the field name.")
          .withConfigProperty("field");
      }

      if (!isEntireRecord()) {
        if (!containsMacro("field")) {
          if (inputSchema.getField(getFieldName()) == null) {
            collector.addFailure("Field specified is not present in the input schema", "")
              .withConfigProperty("field");
          }


          Schema.Type type = inputSchema.getField(getFieldName()).getSchema().getType();
          if (!type.isSimpleType() || type.equals(Schema.Type.BYTES)) {
            collector.addFailure("Filtering on field supports only basic types " +
                                   "(string, bool, int, long, float, double)", "").withConfigProperty("field");
          }
        }
      }

      String templateName = String.format("projects/%s/inspectTemplates/%s", getProject(), templateId);
      GetInspectTemplateRequest request = GetInspectTemplateRequest.newBuilder().setName(templateName).build();

      try {
        InspectTemplate template = client.getInspectTemplate(request);

      } catch (Exception e) {
        collector.addFailure("Unable to validate template name.", "Ensure template ID matches the specified ID in DLP")
          .withConfigProperty("template-id");
      }

    }
  }
}
