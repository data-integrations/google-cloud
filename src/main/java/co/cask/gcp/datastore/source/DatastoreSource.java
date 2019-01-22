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
package co.cask.gcp.datastore.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.gcp.datastore.exception.DatastoreExecutionException;
import co.cask.gcp.datastore.source.util.DatastoreSourceQueryUtil;
import co.cask.gcp.datastore.source.util.DatastoreSourceSchemaUtil;
import co.cask.gcp.datastore.util.DatastoreUtil;
import co.cask.hydrator.common.LineageRecorder;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.ws.rs.Path;

/**
 * Batch Datastore Source Plugin reads the data from Google Cloud Datastore.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(DatastoreSource.NAME)
@Description("Google Cloud Datastore is a NoSQL document database built for automatic scaling and high performance. "
  + "Source plugin provides ability to read data from it by Kind with various filters usage.")
public class DatastoreSource extends BatchSource<NullWritable, Entity, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreSource.class);
  public static final String NAME = "Datastore";

  private final DatastoreSourceConfig config;
  private Schema schema;

  public DatastoreSource(DatastoreSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    LOG.debug("Validate config during `configurePipeline` stage: {}", config);
    config.validate();
    if (!config.containsMacro("schema")) {
      pipelineConfigurer.getStageConfigurer().setOutputSchema(config.getSchema());
    }
  }

  @Override
  public void prepareRun(BatchSourceContext batchSourceContext) {
    LOG.debug("Validate config during `prepareRun` stage: {}", config);
    config.validate();

    batchSourceContext.setInput(Input.of(config.getReferenceName(), new DatastoreInputFormatProvider(config)));

    Schema schema = config.getSchema();
    LineageRecorder lineageRecorder = new LineageRecorder(batchSourceContext, config.getReferenceName());
    lineageRecorder.createExternalDataset(schema);
    lineageRecorder.recordRead("Read", "Read from Datastore.",
       Objects.requireNonNull(schema.getFields()).stream()
         .map(Schema.Field::getName)
         .collect(Collectors.toList()));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    schema = config.getSchema();
  }

  @Override
  public void transform(KeyValue<NullWritable, Entity> input, Emitter<StructuredRecord> emitter) {
    StructuredRecord.Builder builder = StructuredRecord.builder(schema);
    List<Schema.Field> fields = Objects.requireNonNull(schema.getFields());
    Entity entity = input.getValue();

    for (Schema.Field field : fields) {
      String fieldName = field.getName();

      if (config.isIncludeKey() && fieldName.equals(config.getKeyAlias())) {
        builder.set(fieldName, DatastoreSourceQueryUtil.transformKeyToKeyString(entity.getKey(), config.getKeyType()));
        continue;
      }

      populateRecordBuilder(builder, entity, field.getName(), field.getSchema());
    }

    emitter.emit(builder.build());
  }

  private void populateRecordBuilder(StructuredRecord.Builder builder, FullEntity<?> entity,
                                     String fieldName, Schema fieldSchema) {
    if (!entity.contains(fieldName) || entity.isNull(fieldName)) {
      if (!fieldSchema.isNullable()) {
        throw new IllegalArgumentException("Can not set null value to a not null field : " + fieldName);
      }
      builder.set(fieldName, null);
      return;
    }

    Schema.Type fieldType = fieldSchema.getType();
    switch (fieldType) {
      case STRING:
        builder.set(fieldName, entity.getString(fieldName));
        break;
      case DOUBLE:
        builder.set(fieldName, entity.getDouble(fieldName));
        break;
      case BOOLEAN:
        builder.set(fieldName, entity.getBoolean(fieldName));
        break;
      case LONG:
        Schema.LogicalType logicalType = fieldSchema.getLogicalType();
        if (logicalType == null) {
          builder.set(fieldName, entity.getLong(fieldName));
        } else if (Schema.LogicalType.TIMESTAMP_MICROS == logicalType) {
          // GC timestamp supports nano second level precision, CDAP only micro second level precision
          Timestamp timestamp = entity.getTimestamp(fieldName);
          Instant instant = Instant.ofEpochSecond(timestamp.getSeconds()).plusNanos(timestamp.getNanos());
          builder.setTimestamp(fieldName,
                               ZonedDateTime.ofInstant(instant, ZoneId.ofOffset("UTC", ZoneOffset.UTC)));
        } else {
          throw new IllegalArgumentException("Unsupported logical type for Long: " + logicalType);
        }
        break;
      case BYTES:
        builder.set(fieldName, entity.getBlob(fieldName).toByteArray());
        break;
      case RECORD:
        FullEntity<?> nestedEntity = entity.getEntity(fieldName);
        StructuredRecord.Builder nestedBuilder = StructuredRecord.builder(fieldSchema);
        Objects.requireNonNull(fieldSchema.getFields()).forEach(
          nestedField -> populateRecordBuilder(nestedBuilder, nestedEntity,
                                               nestedField.getName(), nestedField.getSchema()));
        builder.set(fieldName, nestedBuilder.build());
        break;
      case UNION:
        // nullable fields in CDAP are represented as UNION of NULL and FIELD_TYPE
        if (fieldSchema.isNullable()) {
          populateRecordBuilder(builder, entity, fieldName, fieldSchema.getNonNullable());
          break;
        }
        throw new IllegalArgumentException("Complex UNION type is not supported");
      default:
        throw new IllegalArgumentException("Unexpected field type: " + fieldType);
    }
  }

  /**
   * Validates given configuration values needed to construct query to obtain schema.
   * Constructs query using namespace, kind and ancestors (if present), limiting query to return only one row.
   * Will fail if query does not return any rows.
   * Note: not including filter properties since their type depends on the schema
   *
   * @param config Datastore configuration
   * @return CDAP schema
   */
  @Path("getSchema")
  public Schema getSchema(DatastoreSourceConfig config) {
    EntityQuery.Builder queryBuilder = Query.newEntityQueryBuilder()
      .setNamespace(config.getNamespace())
      .setKind(config.getKind())
      .setLimit(1);

    Key ancestorKey = DatastoreSourceQueryUtil.constructAncestorKey(config);
    if (ancestorKey != null) {
      queryBuilder.setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorKey));
    }
    EntityQuery query = queryBuilder.build();
    LOG.debug("Executing query for `Get Schema`: {}", query);

    Datastore datastore = DatastoreUtil.getDatastore(config.getServiceAccountFilePath(), config.getProject());
    QueryResults<Entity> results = datastore.run(query);

    if (results.hasNext()) {
      Entity entity = results.next();
      return DatastoreSourceSchemaUtil.constructSchema(entity, config);
    }
    throw new DatastoreExecutionException("Datastore query did not return any results. "
      + "Please check Namespace, Kind and Ancestor properties.");
  }
}
