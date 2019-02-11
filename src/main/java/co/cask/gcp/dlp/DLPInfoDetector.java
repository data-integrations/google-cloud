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

package co.cask.gcp.dlp;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.gcp.common.GCPConfig;
import co.cask.gcp.common.GCPUtils;
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

/**
 * Cloud DLP.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(DLPInfoDetector.NAME)
@Description(DLPInfoDetector.DESCRIPTION)
public final class DLPInfoDetector extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(DLPInfoDetector.class);
  public static final String NAME = "DLPInfoDetector";
  public static final String DESCRIPTION = "Cloud DLP inspects data based on configured information types.";

  // Stores the configuration passed to this class from user.
  private final Config config;

  private DlpServiceClient client;
  private InspectConfig inspectConfig;

  private static final Schema DLP_INFO_TYPE_SCHEMA =
    Schema.recordOf("dlpinfotypes",
                    Schema.Field.of("quote", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("infotype", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("likelihood", Schema.nullableOf(Schema.of(Schema.Type.STRING)))
    );



  @VisibleForTesting
  public DLPInfoDetector(Config config) {
    this.config = config;
  }

  /**
   * Confiigure Pipeline.
   * @param pipelineConfigurer
   * @throws IllegalArgumentException
   */
  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    if (!config.containsMacro(config.getFieldName()) && inputSchema.getField(config.getFieldName()) == null) {
      throw new IllegalArgumentException("Field specified is not present in input schema");
    }
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    // Macros are resolved by this time, so we check before runninig if the input field specified by user
    // is present in the input schema.
    if (context.getInputSchema().getField(config.getFieldName()) == null) {
      throw new IllegalArgumentException("Input field not present in the input schema");
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
    client = DlpServiceClient.create(getSettings());
    inspectConfig =
      InspectConfig.newBuilder()
        .addAllInfoTypes(config.getInfoTypes())
        .setMinLikelihood(config.getMinLikelihood())
        .setLimits(InspectConfig.FindingLimits.newBuilder().setMaxFindingsPerItem(config.getMaxFindings()).build())
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
    if (object instanceof String) {
      ByteContentItem byteContentItem =
        ByteContentItem.newBuilder()
          .setType(ByteContentItem.BytesType.TEXT_UTF8)
          .setData(ByteString.copyFromUtf8(String.valueOf(object)))
          .build();
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
          outputBuilder.set("infotype", finding.getInfoType().getName());
          outputBuilder.set("likelihood", finding.getLikelihood().getValueDescriptor().getName());
          emitter.emit(outputBuilder.build());
        }
      }
    }
  }

  /**
   * Destroy.
   */
  @Override
  public void destroy() {
    super.destroy();
  }

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
