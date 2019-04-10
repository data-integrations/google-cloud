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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.DlpServiceSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.privacy.dlp.v2.ByteContentItem;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.Finding;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.InspectResult;
import com.google.privacy.dlp.v2.Likelihood;
import com.google.privacy.dlp.v2.ProjectName;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.Path;

/**
 * This class <code>SensitiveRecordInspector</code> inspects a input field to detect type of sensitive data.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(SensitiveRecordInspector.NAME)
@Description(SensitiveRecordInspector.DESCRIPTION)
public final class SensitiveRecordInspector extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SensitiveRecordInspector.class);
  public static final String NAME = "SensitiveRecordInspector";
  public static final String DESCRIPTION = "Cloud DLP data inspector for identifying sensitive data.";

  // Stores the configuration passed to this class from user.
  private final Config config;
  // An instance of Dlp service client.
  private DlpServiceClient client;
  // Holds the configuration for inspector.
  private InspectConfig inspectConfig;
  // Defines the output schema.
  private static final Schema DLP_INFO_TYPE_SCHEMA =
    Schema.recordOf("sensitivedata",
                    Schema.Field.of("quote", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("type", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("likelihood", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );


  @VisibleForTesting
  public SensitiveRecordInspector(Config config) {
    this.config = config;
  }

  /**
   * Confiigure Pipeline.
   *
   * @param pipelineConfigurer
   * @throws IllegalArgumentException
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (!config.containsMacro(config.getFieldName()) && inputSchema.getField(config.getFieldName()) == null) {
      throw new IllegalArgumentException("Field specified for inspection is not present in input schema");
    }
  }

  /**
   *
   * @param context
   * @throws Exception
   */
  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    // Macros are resolved by this time, so we check before runninig if the input field specified by user
    // is present in the input schema.
    if (context.getInputSchema().getField(config.getFieldName()) == null) {
      throw new IllegalArgumentException("Field specified for inspection is not present in input schema");
    }
  }

  /**
   * Initialize.
   *
   * @param context
   * @throws Exception
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    client = DlpServiceClient.create(getDlpSettings());
    inspectConfig =
      InspectConfig.newBuilder()
        .addAllInfoTypes(config.getInfoTypes())
        .setMinLikelihood(config.getMinLikelihood())
        .setLimits(InspectConfig.FindingLimits.newBuilder()
                     .setMaxFindingsPerItem(config.getMaxFindings())
                     .build())
        .setIncludeQuote(config.getIncludeQuote())
        .build();
  }

  /**
   * Transform.
   *
   * @param record
   * @param emitter
   * @throws Exception
   */
  @Override
  public void transform(StructuredRecord record, Emitter<StructuredRecord> emitter) throws Exception {
    Object object = record.get(config.getFieldName());

    ByteContentItem byteContentItem = null;
    if (object instanceof String) {
      byteContentItem =
        ByteContentItem.newBuilder()
          .setType(ByteContentItem.BytesType.TEXT_UTF8)
          .setData(ByteString.copyFromUtf8(String.valueOf(object)))
          .build();
    } else if (object instanceof byte[]) {
      byteContentItem =
        ByteContentItem.newBuilder()
          .setType(ByteContentItem.BytesType.TEXT_UTF8)
          .setData(ByteString.copyFrom((byte[]) object))
          .build();
    }

    if (byteContentItem == null) {
      ContentItem contentItem = ContentItem.newBuilder().setByteItem(byteContentItem).build();
      InspectContentRequest request =
        InspectContentRequest.newBuilder()
          .setParent(ProjectName.of(config.getProject()).toString())
          .setInspectConfig(inspectConfig)
          .setItem(contentItem)
          .build();
      InspectContentResponse response = client.inspectContent(request);
      InspectResult result = response.getResult();

      if (result.getFindingsCount() > 0) {
        for (Finding finding : result.getFindingsList()) {
          StructuredRecord.Builder outputBuilder = StructuredRecord.builder(DLP_INFO_TYPE_SCHEMA);
          if (config.getIncludeQuote()) {
            outputBuilder.set("quote", finding.getQuote());
          }
          outputBuilder.set("type", finding.getInfoType().getName());
          outputBuilder.set("likelihood", finding.getLikelihood().getValueDescriptor().getName());
          emitter.emit(outputBuilder.build());
        }
      }
    }
  }

  /**
   * Configures the <code>DlpSettings</code> to use user specified service account file or auto-detect.
   *
   * @return Instance of <code>DlpServiceSettings</code>
   * @throws IOException thrown when there is issue reading service account file.
   */
  private DlpServiceSettings getDlpSettings() throws IOException {
    DlpServiceSettings.Builder builder = DlpServiceSettings.newBuilder();
    if (config.getServiceAccountFilePath() != null) {
      builder.setCredentialsProvider(
        () -> GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath())
      );
    }
    return builder.build();
  }

  /**
   * Request object from the Plugin REST call.
   */
  public static final class Request extends Config {
    private Schema inputSchema;
  }

  /**
   * This method returns the expected output schema for <code>SensitiveRecordInspector</code> plugin.
   *
   * @param request user specified input schema.
   * @return Output <code>Schema</code> object.
   */
  @Path("getSchema")
  public Schema getSchema(Request request) {
    return DLP_INFO_TYPE_SCHEMA;
  }

  /**
   * This class <code>Config</code> specifies the configuration used by <code>SensitiveRecordInspector</code>.
   */
  public static class Config extends GCPConfig {
    @Macro
    @Name("field")
    @Description("Name of field to be inspected")
    private String field;

    @Macro
    @Name("max-findings")
    @Description("Maximum number of findings to report (0=maximum supported)")
    private int maxFindings;

    @Macro
    @Name("minimum-likelihood")
    @Description("The minimum likelihood required for returning a match.")
    private String minLikelihood;

    @Macro
    @Name("include-quote")
    @Description("Whether to include matching string")
    private String includeQuote;

    @Macro
    @Name("info-types")
    @Description("Information types to be matched")
    private String infoTypes;

    public String getFieldName() {
      return field;
    }

    public List<InfoType> getInfoTypes() {
      String[] types = infoTypes.split(",");
      List<InfoType> list = new ArrayList<>();
      for (String type : types) {
        list.add(InfoType.newBuilder().setName(type).build());
      }
      return list;
    }

    public boolean getIncludeQuote() {
      if (includeQuote.equalsIgnoreCase("yes")) {
        return true;
      } else {
        return false;
      }
    }

    public Likelihood getMinLikelihood() {
      return Likelihood.POSSIBLE;
    }

    public int getMaxFindings() {
      return maxFindings;
    }
  }
}
