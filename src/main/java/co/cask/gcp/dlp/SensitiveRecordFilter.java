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
import co.cask.cdap.etl.api.InvalidEntry;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.StageSubmitterContext;
import co.cask.cdap.etl.api.Transform;
import co.cask.cdap.etl.api.TransformContext;
import co.cask.cdap.format.StructuredRecordStringConverter;
import co.cask.gcp.common.GCPConfig;
import co.cask.gcp.common.GCPUtils;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.DlpServiceSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
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
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class implements a Cloud DLP sensitive record filter.
 * The class uses the streaming DLP API to analyze a field or entire record for sensitivity.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(SensitiveRecordFilter.NAME)
@Description(SensitiveRecordFilter.DESCRIPTION)
public final class SensitiveRecordFilter extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(SensitiveRecordFilter.class);
  public static final String NAME = "SensitiveRecordFilter";
  public static final String DESCRIPTION = "Filters input records based that are sensitive.";

  // Stores the configuration passed to this class from user.
  private final Config config;

  // DLP service client for managing interactions with DLP service.
  private DlpServiceClient client;
  private InspectConfig inspectConfig;

  @VisibleForTesting
  public SensitiveRecordFilter(Config config) {
    this.config = config;
  }

  /**
   * Invoked during deployment of pipeline to validate configuration of the pipeline.
   * This method checks if the input specified is 'field' type and if it is, then checks
   * if the field specified is present in the input schema.
   *
   * @param configurer A handle to the entire pipeline configuration.
   * @throws IllegalArgumentException if there any issues with configuration of the plugin.
   */
  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    super.configurePipeline(configurer);
    Schema inputSchema = configurer.getStageConfigurer().getInputSchema();
    if (!config.containsMacro("entire-record") && !config.isEntireRecord() && config.getFieldName() == null) {
      throw new IllegalArgumentException("Input type is specified as 'Field', " +
                                           "but a field name has not been specified. Specify the field name.");
    }
    if (!config.containsMacro("field") && inputSchema.getField(config.getFieldName()) == null) {
      throw new IllegalArgumentException("Field specified is not present in the input schema");
    }
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    // Macros are resolved by this time, so we check before runninig if the input field specified by user
    // is present in the input schema.
    if (!config.isEntireRecord() && config.getFieldName() == null) {
      throw new IllegalArgumentException("Input type is specified as 'Field', " +
                                           "but a field name has not been specified.");
    }
    if (context.getInputSchema().getField(config.getFieldName()) == null) {
      throw new IllegalArgumentException("Field specified is not present in the input schema. " +
                                           "Please specify an input field that is in the input.");
    }
  }

  /**
   * Initialize this <code>SensitiveRecordFilter</code> plugin.
   * A instance of DLP client is creates with mapped infotypes.
   *
   * @param context
   * @throws Exception
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    SensitiveDataMapping sensitivityMapping = new SensitiveDataMapping();
    List<InfoType> sensitiveInfoTypes = sensitivityMapping.getSensitiveInfoTypes(config.getSensitiveTypes());
    client = DlpServiceClient.create(getSettings());
    inspectConfig =
      InspectConfig.newBuilder()
        .addAllInfoTypes(sensitiveInfoTypes)
        .setMinLikelihood(config.getFilterConfidence())
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
    Object object = null;
    ContentItem contentItem = null;

    if (!config.isEntireRecord()) {
      object = record.get(config.getFieldName());
    } else {
      object = StructuredRecordStringConverter.toDelimitedString(record, ",");
    }

    if (object instanceof String) {
      ByteContentItem byteContentItem =
        ByteContentItem.newBuilder()
          .setType(ByteContentItem.BytesType.TEXT_UTF8)
          .setData(ByteString.copyFromUtf8(String.valueOf(object)))
          .build();
      contentItem = ContentItem.newBuilder().setByteItem(byteContentItem).build();
    } else if (object instanceof byte[]) {
      ByteContentItem byteContentItem =
        ByteContentItem.newBuilder()
          .setType(ByteContentItem.BytesType.TEXT_UTF8)
          .setData(ByteString.copyFrom((byte[]) object))
          .build();
      contentItem = ContentItem.newBuilder().setByteItem(byteContentItem).build();
    }

    InspectContentRequest request =
      InspectContentRequest.newBuilder()
        .setParent(ProjectName.of(config.getProject()).toString())
        .setInspectConfig(inspectConfig)
        .setItem(contentItem)
        .build();
    InspectContentResponse response = client.inspectContent(request);
    InspectResult result = response.getResult();

    if (result.getFindingsCount() > 0) {
      List<String> findingInfoTypes = new ArrayList<>();
      for (Finding finding : result.getFindingsList()) {
        if (canFilter(finding.getLikelihood(), config.getFilterConfidence())) {
          findingInfoTypes.add(finding.getInfoType().getName());
        }
      }
      if (findingInfoTypes.size() > 0) {
        List<String> dedup = Lists.newArrayList(Sets.newHashSet(findingInfoTypes));
        emitter.emitError(new InvalidEntry<>(dedup.size(), String.join(",", dedup), record));
        return;
      }
    }
    emitter.emit(record);
  }

  /**
   * 0 1 false
   * 1 1 true
   * 2 1 true
   * 0 0 true
   * 1 0 true
   * 2 0 true
   * 0 2 false
   * 1 2 false
   * 2 2 true
   * @param inputLikelihood
   * @param configLikelihood
   * @return
   */
  private boolean canFilter(Likelihood inputLikelihood, Likelihood configLikelihood) {
    Map<String, Integer> mapping = ImmutableMap.of(
      "POSSIBLE", 0,
      "LIKELY", 1,
      "VERY_LIKELY", 2
    );

    int left = mapping.get(inputLikelihood.name());
    int right = mapping.get(configLikelihood.name());
    if (left >= right) {
      return true;
    }
    return false;
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
    @Description("Entire record or just an individual field")
    private boolean entireRecord;

    @Macro
    @Name("field")
    @Description("Name of field to be inspected")
    @Nullable
    private String field;

    @Macro
    @Name("filter-confidence")
    @Description("Confidence in types ")
    private String filterConfidence;

    @Macro
    @Name("sensitive-types")
    @Description("Information types to be matched")
    private String sensitiveTypes;

    /**
     * @return The name of field that needs to be inspected for sensitive data.
     */
    public String getFieldName() {
      return field;
    }

    /**
     * @return Array of composite sensitive types specified by user.
     */
    public String[] getSensitiveTypes() {
      String[] types = sensitiveTypes.split(",");
      return types;
    }

    /**
     * @return {@link InfoType} based on user filter confidence.
     */
    public Likelihood getFilterConfidence() {
      if (filterConfidence.equalsIgnoreCase("low")) {
        return Likelihood.POSSIBLE;
      } else if (filterConfidence.equalsIgnoreCase("medium")) {
        return Likelihood.LIKELY;
      }
      return Likelihood.VERY_LIKELY;
    }

    /**
     * @return true if entire record to checked for sensitive data.
     */
    public boolean isEntireRecord() {
      return entireRecord;
    }
  }
}
