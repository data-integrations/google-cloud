package io.cdap.plugin.gcp.bigquery.fctransform;


import ai.festcloud.datafabric.plugins.common.integrity.MetadataUtils;
import ai.festcloud.metadata.model.TypeField;
import ai.festcloud.metadata.model.TypeRecord;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnectorConfig;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * BigQuery Mdm Integrity Validation config
 */
public class MdmIntegrityBigQueryTransformerConfig extends PluginConfig {

  public static final String MAPPING = "mapping";
  public static final String FULLY_QUALIFIED_ENTITY_NAME = "fullyQualifiedEntityName";
  public static final String SCHEMA = "schema";
  public static final String MANIFEST_VERSION = "manifestVersion";


  @Name(ConfigUtil.NAME_CONNECTION)
  @Macro
  @Nullable
  @Description("The existing connection to use.")
  public BigQueryConnectorConfig connection;


  @Name(ConfigUtil.NAME_USE_CONNECTION)
  @Nullable
  @Description("Whether to use an existing connection.")
  public Boolean useConnection;

  @Name(MAPPING)
  @Description("Properties to validate")
  @Macro
  private final String mapping;

  @Name(FULLY_QUALIFIED_ENTITY_NAME)
  @Description("Entity name with namespace")
  @Macro
  private final String fullyQualifiedEntityName;

  @Name(MANIFEST_VERSION)
  @Description("Metadata manifest version")
  @Macro
  private final String manifestVersion;

  @Name("fcidRequired")
  @Description("Indicates whether FestCloudID is required. "
      + "If true, records without a FestCloudID are sent to the error flow. "
      + "If false, records without a FestCloudID are processed and not sent to the error flow.")
  @Macro
  private final Boolean fcidRequired;

  @Name(SCHEMA)
  @Description("Schema of the output records.")
  @Macro
  private final String schema;


  public MdmIntegrityBigQueryTransformerConfig(BigQueryConnectorConfig connection,
                                               Boolean useConnection, String mapping,
                                               String fullyQualifiedEntityName, String manifestVersion,
                                               Boolean fcidRequired, String schema) {
    this.connection = connection;
    this.useConnection = useConnection;
    this.mapping = mapping;
    this.fullyQualifiedEntityName = fullyQualifiedEntityName;
    this.manifestVersion = manifestVersion;
    this.fcidRequired = fcidRequired;
    this.schema = schema;
  }


  public void validate(FailureCollector collector, Map<String, TypeRecord> entities,
                       Schema outputSchema) {
    ConfigUtil.validateConnection(this, useConnection, connection, collector);
    TypeRecord typeRecord = entities.get(fullyQualifiedEntityName);
    List<TypeField> fields = typeRecord.getFields();

    fields.stream().filter(MetadataUtils::integrityRequired).map(MetadataUtils::getFieldName)
        .forEach(fieldName -> validateField(fieldName, outputSchema, collector));
  }

  private void validateField(String fieldName, Schema outputSchema, FailureCollector collector) {
    if (outputSchema.getField(fieldName) == null) {
      collector.addFailure(String.format("Can't find field %s in output record", fieldName),
                           String.format(
                               "Field %s mandatory for integrity validation according to metadata definition",
                               fieldName));
    }
  }

  public Schema getSchema(FailureCollector collector) {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure(String.format("Failed to parse schema: %s", schema), null);
      throw collector.getOrThrowException();
    }
  }

  @Nullable
  public BigQueryConnectorConfig getConnection() {
    return connection;
  }

  public void setConnection(@Nullable BigQueryConnectorConfig connection) {
    this.connection = connection;
  }

  @Nullable
  public Boolean getUseConnection() {
    return useConnection;
  }

  public void setUseConnection(@Nullable Boolean useConnection) {
    this.useConnection = useConnection;
  }

  public String getFullyQualifiedEntityName() {
    return fullyQualifiedEntityName;
  }

  public String getManifestVersion() {
    return manifestVersion;
  }

  public Boolean getFcidRequired() {
    return fcidRequired;
  }

  public String getSchema() {
    return schema;
  }

  public String getMapping() {
    return mapping;
  }
}

