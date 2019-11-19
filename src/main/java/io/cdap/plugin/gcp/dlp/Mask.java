package io.cdap.plugin.gcp.dlp;

import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.DlpServiceSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.RecordTransformations;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.avro.generic.GenericData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Mask class.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(Mask.NAME)
@Description(Mask.DESCRIPTION)
public class Mask extends Transform<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(Mask.class);
  public static final String NAME = "Mask";
  public static final String DESCRIPTION = "Mask fields";

  // Stores the configuration passed to this class from user.
  private final Config config;

  // DLP service client for managing interactions with DLP service.
  private DlpServiceClient client;

  private RecordTransformations recordTransformations;

  @VisibleForTesting
  public Mask(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    config.validate(stageConfigurer.getFailureCollector(), stageConfigurer.getInputSchema());

    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    client = DlpServiceClient.create(getSettings());
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);

    //Construct RecordTransformations object to be used for transforms
    this.recordTransformations = constructRecordTransformations();

    // FieldTransformOperation fieldTransformOperation = new FieldTransformOperation("Rename", "whataefer",
    //                                                                               Collections.singletonList("a"), "a1");
    // FieldTransformOperation fieldTransformOperation = new FieldTransformOperation("Identity transform 1", "whataefer",
    //                                                                               Collections.singletonList("c"), "c");
    // FieldTransformOperation fieldTransformOperation = new FieldTransformOperation("Identity transform 1", "whataefer",
    //                                                                               Collections.singletonList("d"), "d");
    // context.record(fieldTransformOperation);
  }

  private RecordTransformations constructRecordTransformations() throws Exception {
    RecordTransformations.Builder recordTransformationsBuilder = RecordTransformations.newBuilder();

    List<DlpFieldTransformationConfig> transformationConfigs = config.parseTransformations();

    recordTransformationsBuilder.addAllFieldTransformations(
      transformationConfigs.stream()
                           .map(DlpFieldTransformationConfig::toFieldTransformation)
                           .collect(Collectors.toList())
    );

    return recordTransformationsBuilder.build();

  }

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {

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
   * Config
   */
  public static class Config extends GCPConfig {

    @Macro
    private String fieldsToTransform;


    public List<DlpFieldTransformationConfig> parseTransformations() throws Exception {
      Gson gson = new GsonBuilder()
        .registerTypeAdapter(DlpFieldTransformationConfig.class, new DlpFieldTransformationConfigCodec())
        .create();
      String[] values = gson.fromJson(fieldsToTransform, String[].class);
      List<DlpFieldTransformationConfig> transformationConfigs = new ArrayList<>();
      for (String value : values) {
        transformationConfigs.add(gson.fromJson(value, DlpFieldTransformationConfig.class));
      }
      return transformationConfigs;
    }

    ;

    public void validate(FailureCollector collector, Schema inputSchema) {

      try {
        List<DlpFieldTransformationConfig> transformationConfigs = parseTransformations();

        for (DlpFieldTransformationConfig config : transformationConfigs) {

          config.validate(collector, inputSchema);

        }
      } catch (Exception e) {
        collector.addFailure(String.format("Error while parsing transforms: %s", e.getMessage()), "")
                 .withConfigProperty("fieldsToTransform");
      }


    }


  }
}
