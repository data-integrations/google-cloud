package io.cdap.plugin.gcp.dlp;

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

    DeidentifyContentResponse response = client.deidentifyContent(requestBuilder.build());
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
      Schema fieldSchema = oldRecord.getSchema().getField(fieldName).getSchema().getNonNullable();
      if (fieldValue == null) {
        continue;
      }

      if (fieldSchema.isSimpleOrNullableSimple()) {
        recordBuilder.convertAndSet(fieldName, fieldValue.getStringValue());
      } else {
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
              LocalDateTime localDateTime = Instant
                .ofEpochSecond(timestampValue.getSeconds(), timestampValue.getNanos())
                .atZone(zoneId)
                .toLocalDateTime();
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


        }

      }
    }
    return recordBuilder.build();
  }

  private StructuredRecord.Builder createBuilderFromStructuredRecord(StructuredRecord record) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(record.getSchema());
    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.getName();
      Object fieldValue = record.get(fieldName);
      Schema fieldSchema = field.getSchema().getNonNullable();

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
      final Schema.LogicalType logicalType = field.getSchema().getNonNullable().getLogicalType();
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

        final Schema.Type type = field.getSchema().getNonNullable().getType();
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
          for (DlpFieldTransformationConfig config : transformationConfigs) {

            if (!customTemplateEnabled && Arrays.asList(config.getFilters()).contains("NONE")) {
              collector.addFailure(String.format("The '%s' transform on '%s' depends on a custom template.",
                                                 config.getTransform(), String.join(", ", config.getFields())),
                                   "Enable the custom template option and provide the name of it.");
            }
            config.validate(collector, inputSchema, FIELDS_TO_TRANSFORM);
            Gson gson = new Gson();
            for (String field : config.getFields()) {
              for (String filter : config.getFilterDisplayNames()) {
                String transformKey = String.format("%s:%s", field, filter);
                if (transforms.containsKey(transformKey)) {
                  ErrorConfig errorConfig = config.getErrorConfig("");
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
                  collector.addFailure(errorMessage, "")
                           .withConfigElement(FIELDS_TO_TRANSFORM, gson.toJson(errorConfig));
                } else {
                  transforms.put(transformKey, config.getTransform());
                }
              }
            }

          }
        } catch (Exception e) {
          collector.addFailure(String.format("Error while parsing transforms: %s", e.getMessage()), "")
                   .withConfigProperty(FIELDS_TO_TRANSFORM);
        }
      }


    }


  }
}
