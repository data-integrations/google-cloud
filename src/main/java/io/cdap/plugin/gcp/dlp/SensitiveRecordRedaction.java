package io.cdap.plugin.gcp.dlp;

import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.DlpServiceSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.GetInspectTemplateRequest;
import com.google.privacy.dlp.v2.InspectTemplate;
import com.google.privacy.dlp.v2.RecordTransformations;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import com.google.type.TimeOfDay;
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
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.dlp.configs.DlpFieldTransformationConfig;
import io.cdap.plugin.gcp.dlp.configs.DlpFieldTransformationConfigCodec;
import io.cdap.plugin.gcp.dlp.configs.ErrorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * SensitiveRecordRedaction class.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(SensitiveRecordRedaction.NAME)
@Description(SensitiveRecordRedaction.DESCRIPTION)
public class SensitiveRecordRedaction extends Transform<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SensitiveRecordRedaction.class);
  public static final String NAME = "SensitiveRecordRedaction";
  public static final String DESCRIPTION = "SensitiveRecordRedaction fields";
  public static final int MAX_RETRIES = 5;

  private StageMetrics metrics;

  // Stores the configuration passed to this class from user.
  private final Config config;

  // DLP service client for managing interactions with DLP service.
  private DlpServiceClient client;

  private RecordTransformations recordTransformations;

  @VisibleForTesting
  public SensitiveRecordRedaction(Config config) {
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
    metrics = context.getMetrics();
    client = DlpServiceClient.create(getSettings());
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    config.validate(context.getFailureCollector(), context.getInputSchema());
    context.getFailureCollector().getOrThrowException();
    if (config.customTemplateEnabled) {
      String templateName = String.format("projects/%s/inspectTemplates/%s", config.getProject(), config.templateId);
      GetInspectTemplateRequest request = GetInspectTemplateRequest.newBuilder().setName(templateName).build();

      try {
        if (client == null) {
          client = DlpServiceClient.create(getSettings());
        }
        InspectTemplate template = client.getInspectTemplate(request);

      } catch (Exception e) {
        throw new IllegalArgumentException(
          "Unable to validate template name. Ensure template ID matches the specified ID in DLP");
      }
    }

    List<FieldOperation> fieldOperations = getFieldOperations(context.getInputSchema());

    context.record(fieldOperations);
  }

  private List<FieldOperation> getFieldOperations(Schema inputSchema) throws Exception {

    //Parse config into format 'FieldName': List<>(['transform','filter'])
    HashMap<String, List<String[]>> fieldOperationsData = new HashMap<>();
    for (DlpFieldTransformationConfig transformationConfig : config.parseTransformations()) {
      for (String field : transformationConfig.getFields()) {
        String filterName = String.join(", ", transformationConfig.getFilters())
                                  .replace("NONE",
                                           String.format("Custom Template (%s)", config.templateId));

        String transformName = transformationConfig.getTransform();

        if (!fieldOperationsData.containsKey(field)) {
          fieldOperationsData.put(field, Collections.singletonList(new String[]{transformName, filterName}));
        } else {
          fieldOperationsData.get(field).add(new String[]{transformName, filterName});
        }

      }
    }

    for (Schema.Field field : inputSchema.getFields()) {
      if (!fieldOperationsData.containsKey(field.getName())) {
        fieldOperationsData.put(field.getName(), Collections.singletonList(new String[]{"Identity", ""}));
      }
    }

    List<FieldOperation> fieldOperations = new ArrayList<>();

    for (String fieldName : fieldOperationsData.keySet()) {
      StringBuilder descriptionBuilder = new StringBuilder();
      StringBuilder nameBuilder = new StringBuilder();
      descriptionBuilder.append("Applied ");
      boolean first = true;
      for (String[] transformFilterPair : fieldOperationsData.get(fieldName)) {

        String transformName = transformFilterPair[0];
        String filterNames = transformFilterPair[1];

        if (first) {
          descriptionBuilder.append("        ");
        }
        descriptionBuilder.append(String.format("'%s' transform on contents ", transformName));
        if (filterNames.length() > 0) {
          descriptionBuilder.append(" matching ").append(filterNames);
        }
        descriptionBuilder.append(",\n");
        nameBuilder.append(transformName).append(" ,");
        first = false;

      }
      nameBuilder.deleteCharAt(nameBuilder.length() - 1);
      descriptionBuilder.delete(descriptionBuilder.length() - 2, descriptionBuilder.length() - 1);
      nameBuilder.append("on ").append(fieldName);
      fieldOperations
        .add(new FieldTransformOperation(nameBuilder.toString(), descriptionBuilder.toString(),
                                         Collections.singletonList(fieldName), fieldName));
    }
    return fieldOperations;
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
    this.recordTransformations = constructRecordTransformations();
    Table dlpTable = getTableFromStructuredRecord(structuredRecord);

    DeidentifyConfig deidentifyConfig =
      DeidentifyConfig.newBuilder().setRecordTransformations(this.recordTransformations).build();

    ContentItem item = ContentItem.newBuilder().setTable(dlpTable).build();
    DeidentifyContentRequest.Builder requestBuilder = DeidentifyContentRequest.newBuilder()
                                                                              .setParent(
                                                                                "projects/" + config.getProject())
                                                                              .setDeidentifyConfig(deidentifyConfig)
                                                                              .setItem(item);
    if (config.customTemplateEnabled) {
      String templateName = String.format("projects/%s/inspectTemplates/%s", config.getProject(), config.templateId);
      requestBuilder.setInspectTemplateName(templateName);
    }

    DeidentifyContentResponse response = null;
    DeidentifyContentRequest request = requestBuilder.build();
    try {
      metrics.count("redactionTransform.DLPRequests", 1);
      response = client.deidentifyContent(request);
    } catch (Exception e) {
      metrics.count("redactionTransform.failedRequests", 1);
      if (e instanceof ResourceExhaustedException) {
        LOG.error(
          "Failed due to DLP rate limit, please request more quota from DLP: https://cloud.google"
            + ".com/dlp/limits#increases");
      }
      throw e;
    }

    metrics.count("redactionTransform.successfulRequests", 1);
    ContentItem item1 = response.getItem();

    StructuredRecord resultRecord = getStructuredRecordFromTable(item1.getTable(), structuredRecord);

    emitter.emit(resultRecord);

  }

  private StructuredRecord getStructuredRecordFromTable(Table table, StructuredRecord oldRecord) throws Exception {
    StructuredRecord.Builder recordBuilder = createBuilderFromStructuredRecord(oldRecord);

    if (table.getRowsCount() == 0) {
      throw new Exception("DLP returned a table with no rows");
    }
    Table.Row row = table.getRows(0);
    for (int i = 0; i < table.getHeadersList().size(); i++) {

      String fieldName = table.getHeadersList().get(i).getName();

      Value fieldValue = row.getValues(i);
      Schema tempSchema = oldRecord.getSchema().getField(fieldName).getSchema();
      Schema fieldSchema = tempSchema.isNullable() ? tempSchema.getNonNullable() : tempSchema;
      if (fieldValue == null) {
        continue;
      }

      final Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType != null) {
        switch (logicalType) {
          case TIME_MICROS:
          case TIME_MILLIS:

            TimeOfDay timeValue = fieldValue.getTimeValue();
            recordBuilder.setTime(fieldName, LocalTime
              .of(timeValue.getHours(), timeValue.getMinutes(), timeValue.getSeconds(), timeValue.getNanos()));
            break;

          case TIMESTAMP_MICROS:
          case TIMESTAMP_MILLIS:
            Timestamp timestampValue = fieldValue.getTimestampValue();
            ZoneId zoneId = oldRecord.getTimestamp(fieldName).getZone();
            LocalDateTime localDateTime;
            if (timestampValue.getSeconds() + timestampValue.getNanos() == 0) {
              localDateTime = LocalDateTime
                .parse(fieldValue.getStringValue(), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'"));

            } else {
              localDateTime = Instant
                .ofEpochSecond(timestampValue.getSeconds(), timestampValue.getNanos())
                .atZone(zoneId)
                .toLocalDateTime();
            }

            recordBuilder.setTimestamp(fieldName, ZonedDateTime.of(localDateTime, zoneId));
            break;
          case DATE:
            Date dateValue = fieldValue.getDateValue();
            recordBuilder
              .setDate(fieldName, LocalDate.of(dateValue.getYear(), dateValue.getMonth(), dateValue.getDay()));
            break;
          default:
            throw new IllegalArgumentException("Failed to parse table into structured record");

        }


      } else {
        recordBuilder.convertAndSet(fieldName, fieldValue.getStringValue());
      }
    }
    return recordBuilder.build();
  }

  private StructuredRecord.Builder createBuilderFromStructuredRecord(StructuredRecord record) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(record.getSchema());
    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.getName();
      Object fieldValue = record.get(fieldName);
      Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();

      final Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (fieldSchema.getType().isSimpleType()) {
        recordBuilder.set(fieldName, fieldValue);
      } else {
        if (logicalType != null) {
          switch (logicalType) {
            case TIME_MICROS:
            case TIME_MILLIS:
              recordBuilder.setTime(fieldName, (LocalTime) fieldValue);
              break;
            case TIMESTAMP_MICROS:
            case TIMESTAMP_MILLIS:
              recordBuilder.setTimestamp(fieldName, (ZonedDateTime) fieldValue);
              break;
            case DATE:
              recordBuilder.setDate(fieldName, (LocalDate) fieldValue);
              break;
            default:
              throw new IllegalArgumentException(
                String
                  .format("DLP plugin does not support type '%s' for field '%s'", logicalType.toString(), fieldName));

          }
        }
      }
    }
    return recordBuilder;
  }

  private Table getTableFromStructuredRecord(StructuredRecord record) throws Exception {
    Table.Builder tableBuiler = Table.newBuilder();
    Table.Row.Builder rowBuilder = Table.Row.newBuilder();

    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.getName();
      Object fieldValue = record.get(fieldName);
      if (fieldValue == null) {
        continue;
      }

      tableBuiler.addHeaders(FieldId.newBuilder().setName(fieldName).build());

      Value.Builder valueBuilder = Value.newBuilder();
      final Schema fieldSchema =
        field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
      final Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType != null) {
        switch (logicalType) {
          case TIME_MICROS:
          case TIME_MILLIS:
            LocalTime time = record.getTime(fieldName);
            valueBuilder.setTimeValue(
              TimeOfDay.newBuilder()
                       .setHours(time.getHour())
                       .setMinutes(time.getMinute())
                       .setSeconds(time.getSecond())
                       .setNanos(time.getNano())
                       .build()
            );
            break;
          case TIMESTAMP_MICROS:
          case TIMESTAMP_MILLIS:
            ZonedDateTime timestamp = record.getTimestamp(fieldName);
            valueBuilder.setTimestampValue(
              Timestamp.newBuilder()
                       .setSeconds(timestamp.toEpochSecond())
                       .setNanos(timestamp.getNano())
                       .build()
            );
            break;
          case DATE:
            LocalDate date = record.getDate(fieldName);
            valueBuilder.setDateValue(
              Date.newBuilder()
                  .setYear(date.getYear())
                  .setMonth(date.getMonthValue())
                  .setDay(date.getDayOfMonth())
                  .build()
            );
            break;
          default:
            throw new IllegalArgumentException(
              String
                .format("DLP plugin does not support type '%s' for field '%s'", logicalType.toString(), fieldName));


        }
      } else {

        final Schema.Type type = fieldSchema.getType();
        switch (type) {
          case STRING:
            valueBuilder.setStringValue(String.valueOf(fieldValue));
            break;
          case INT:
          case LONG:
            valueBuilder.setIntegerValue((Long) fieldValue);
            break;
          case BOOLEAN:
            valueBuilder.setBooleanValue((Boolean) fieldValue);
            break;
          case DOUBLE:
          case FLOAT:
            valueBuilder.setFloatValue((Double) fieldValue);
            break;
          default:
            throw new IllegalArgumentException(
              String.format("DLP plugin does not support type '%s' for field '%s'", type.toString(), fieldName));

        }
      }

      rowBuilder.addValues(valueBuilder.build());
    }

    tableBuiler.addRows(rowBuilder.build());
    return tableBuiler.build();
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
      builder
        .setCredentialsProvider(() -> GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath()));
    }
    return builder.build();
  }

  /**
   * Config
   */
  public static class Config extends GCPConfig {

    public static final String FIELDS_TO_TRANSFORM = "fieldsToTransform";
    @Macro
    private String fieldsToTransform;

    @Description("Enabling this option will allow you to define a custom DLP Inspection Template to use for matching "
      + "during the transform.")
    private Boolean customTemplateEnabled;

    @Description("ID of the DLP Inspection template")
    @Macro
    @Nullable
    private String templateId;

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


    public void validate(FailureCollector collector, Schema inputSchema) {
      if (customTemplateEnabled) {
        if (!containsMacro("templateId") && templateId == null) {
          collector.addFailure("Must specify template ID in order to use custom template", "")
                   .withConfigProperty("templateId");
        }
      }

      if (fieldsToTransform != null) {
        try {
          List<DlpFieldTransformationConfig> transformationConfigs = parseTransformations();
          HashMap<String, String> transforms = new HashMap<>();
          Boolean firstTransformUsedCustomTemplate = null;
          Boolean anyTransformUsedCustomTemplate = false;
          for (DlpFieldTransformationConfig config : transformationConfigs) {
            ErrorConfig errorConfig = config.getErrorConfig("");
            Gson gson = new Gson();

            //Checking that custom template is defined if it is selected in one of the transforms
            List<String> filters = Arrays.asList(config.getFilters());
            if (!customTemplateEnabled && filters.contains("NONE")) {
              collector.addFailure(String.format("This transform depends on custom template that was not defined.",
                                                 config.getTransform(), String.join(", ", config.getFields())),
                                   "Enable the custom template option and provide the name of it.")
                       .withConfigElement(FIELDS_TO_TRANSFORM, gson.toJson(errorConfig));
            }
            //Validate the config for the transform
            config.validate(collector, inputSchema, FIELDS_TO_TRANSFORM);

            //Check that custom template and built-in types are not mixed
            anyTransformUsedCustomTemplate = anyTransformUsedCustomTemplate || filters.contains("NONE");
            if (firstTransformUsedCustomTemplate == null) {
              firstTransformUsedCustomTemplate = filters.contains("NONE");
            } else {
              if (filters.contains("NONE") != firstTransformUsedCustomTemplate) {
                errorConfig.setTransformPropertyId("filters");
                collector.addFailure("Cannot use custom templates and built-in filters in the same plugin instance.",
                                     "All transforms must use custom templates or built-in filters, not a "
                                       + "combination of both.")
                         .withConfigElement(FIELDS_TO_TRANSFORM, gson.toJson(errorConfig));

              }
            }

            // Make sure the combination of field, transform and filter are unique
            for (String field : config.getFields()) {
              for (String filter : config.getFilterDisplayNames()) {
                String transformKey = String.format("%s:%s", field, filter);
                if (transforms.containsKey(transformKey)) {

                  String errorMessage;
                  if (transforms.get(transformKey).equals(config.getTransform())) {
                    errorMessage = String.format(
                      "Combination of transform, filter and field must be unique. Found multiple definitions for '%s' "
                        + "transform on '%s' with filter '%s'", config.getTransform(), field, filter);

                  } else {
                    errorMessage = String.format(
                      "Only one transform can be defined per field and filter combination. Found conflicting transforms"
                        + " '%s' and '%s'",
                      transforms.get(transformKey), config.getTransform());

                  }
                  errorConfig.setTransformPropertyId("");
                  collector.addFailure(errorMessage, "")
                           .withConfigElement(FIELDS_TO_TRANSFORM, gson.toJson(errorConfig));
                } else {
                  transforms.put(transformKey, config.getTransform());
                }
              }
            }

          }

          // If the user has a custom template enabled but doesnt use it in any of the transforms
          if (!anyTransformUsedCustomTemplate && this.customTemplateEnabled) {
            collector.addFailure("Custom template is enabled but no transforms use a custom template.",
                                 "Please define a transform that uses the custom template or disable the custom "
                                   + "template.")
                     .withConfigProperty("customTemplateEnabled");
          }
        } catch (Exception e) {
          collector.addFailure(String.format("Error while parsing transforms: %s", e.getMessage()), "")
                   .withConfigProperty(FIELDS_TO_TRANSFORM);
        }
      }


    }


  }
}
