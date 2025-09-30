package io.cdap.plugin.gcp.bigquery.fctransform;

import ai.festcloud.datafabric.plugins.common.integrity.CDAPUtils;
import ai.festcloud.datafabric.plugins.common.integrity.IntegrityService;
import ai.festcloud.datafabric.plugins.common.integrity.IntegrityServiceBQ;
import ai.festcloud.datafabric.plugins.common.integrity.MetadataUtils;
import ai.festcloud.datafabric.plugins.common.integrity.mapping.MappingEntryConfig;
import ai.festcloud.datafabric.plugins.common.integrity.mapping.MappingObj;
import ai.festcloud.datafabric.plugins.common.integrity.mapping.MappingParsingService;
import ai.festcloud.metadata.model.TypeRecord;
import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Verify whether the requested values are present in MDM and add new specified field.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("BigQueryMdmIntegrityValidation")
@Description("Verify whether the requested values are present in MDM and add new specified field.")
public class MdmIntegrityBigQueryTransformer extends Transform<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(MdmIntegrityBigQueryTransformer.class);

  private static final String OPERATION = "operation";
  private static final String OPERATION_CREATE = "create";
  private static final String OPERATION_UPDATE = "update";

  private final MdmIntegrityBigQueryTransformerConfig config;
  private Schema outputSchema;

  private MappingObj mapping;
  private Map<String, TypeRecord> entities;
  private IntegrityService integrityService;
  private boolean containsOperationField = false;

  public MdmIntegrityBigQueryTransformer(MdmIntegrityBigQueryTransformerConfig config) {
    this.config = config;
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    LOG.info("Initializing BigQuery integrity validation...");
    super.initialize(context);

    FailureCollector failureCollector = context.getFailureCollector();
    outputSchema = config.getSchema(failureCollector);
    String schemaRegistryHost = context.getArguments().get(MetadataUtils.SCHEMA_REGISTRY_HOST);
    entities = MetadataUtils.getTypeRecordsByManifest(schemaRegistryHost, config.getManifestVersion());
    config.validate(failureCollector, entities, context.getInputSchema());

    MappingParsingService mappingParsingService
            = new MappingParsingService(config.getMapping(),
            config.getFullyQualifiedEntityName(),
            failureCollector,
            entities,
            outputSchema);
    Optional<MappingObj> mappingOpt = mappingParsingService.getMapping();
    mapping = mappingOpt.orElse(null);

    Credentials credentials = config.getConnection().getCredentials(failureCollector);
    BigQuery bigQuery = GCPUtils.getBigQuery(config.getConnection().getProject(), credentials);

    failureCollector.getOrThrowException();

    integrityService = new IntegrityServiceBQ(bigQuery, entities, mapping);
    containsOperationField = outputSchema.getFields()
            .stream().anyMatch(field -> field.getName().equals(OPERATION));

    LOG.info("BigQueryMdmIntegrityValidation initialized.");
  }

  @Override
  public void onRunFinish(boolean succeeded, StageSubmitterContext context) {
    super.onRunFinish(succeeded, context);

  }

  @Override
  public void destroy() {
    super.destroy();
    try {
      integrityService.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    FailureCollector collector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema(collector));
  }


  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter)
          throws Exception {
    try {
      StructuredRecord structuredRecord = fillIds(input);
      emitter.emit(structuredRecord);
    } catch (Exception e) {
      emitter.emitError(new InvalidEntry<>(MetadataUtils.ERROR_CODE, e.getMessage(), input));
    }
  }

  private StructuredRecord fillIds(StructuredRecord input) {

    Map<String, Object> result = new HashMap<>();
    Map<String, List<MappingEntryConfig>> mappingEntryConfigs = mapping.getMappingEntryConfigs();

    mappingEntryConfigs.forEach((targetFieldName, mappingEntryConfig) -> {

      for (MappingEntryConfig entryConfig : mappingEntryConfig) {
        List<String> ids = integrityService.getIds(entryConfig, input);
        if (ids.size() > 1) {
          throw new RuntimeException(
                  "More than one id found for request: " + entryConfig.toString());
        }
        if (ids.size() == 1) {
          result.put(targetFieldName, ids.get(0));
          break;
        }
      }
    });
    if (result.get(MetadataUtils.DEFAULT_TARGET_FIELD) == null && config.isFcpIdRequired()) {
      throw new RuntimeException("ID is required but not provided.");
    }

    return setValuesToTargetFields(input, result);
  }


  private StructuredRecord setValuesToTargetFields(StructuredRecord input,
                                                   Map<String, Object> values) {
    StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
    setFieldValues(input, values, builder, outputSchema);
    return builder.build();
  }

  private void setFieldValues(StructuredRecord input,
                              Map<String, Object> values,
                              StructuredRecord.Builder builder,
                              Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.getName();
      Object fieldValue = input.get(fieldName);

      if (CDAPUtils.isRecordType(field) && fieldValue != null) {
        StructuredRecord nestedRecord = (StructuredRecord) fieldValue;
        Schema nestedSchema = CDAPUtils.getNonNullableSchema(field.getSchema());

        StructuredRecord.Builder nestedBuilder = StructuredRecord.builder(nestedSchema);
        setFieldValues(nestedRecord, values, nestedBuilder, nestedSchema);
        builder.set(fieldName, nestedBuilder.build());
      } else {
        builder.set(fieldName, values.getOrDefault(fieldName, fieldValue));
      }
    }
  }


}
