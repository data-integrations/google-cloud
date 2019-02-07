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
import co.cask.gcp.datastore.util.DatastoreUtil;
import co.cask.hydrator.common.LineageRecorder;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.EntityValue;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.Value;
import com.google.cloud.datastore.ValueType;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private DatastoreSourceTransformer datastoreSourceTransformer;

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
    lineageRecorder.recordRead("Read", "Read from Cloud Datastore.",
       Objects.requireNonNull(schema.getFields()).stream()
         .map(Schema.Field::getName)
         .collect(Collectors.toList()));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    datastoreSourceTransformer = new DatastoreSourceTransformer(config);
  }

  @Override
  public void transform(KeyValue<NullWritable, Entity> input, Emitter<StructuredRecord> emitter) {
    Entity entity = input.getValue();
    StructuredRecord record = datastoreSourceTransformer.transformEntity(entity);
    emitter.emit(record);
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

    Key ancestorKey = constructAncestorKey(config);
    if (ancestorKey != null) {
      queryBuilder.setFilter(StructuredQuery.PropertyFilter.hasAncestor(ancestorKey));
    }
    EntityQuery query = queryBuilder.build();
    LOG.debug("Executing query for `Get Schema`: {}", query);

    Datastore datastore = DatastoreUtil.getDatastore(config.getServiceAccountFilePath(), config.getProject());
    QueryResults<Entity> results = datastore.run(query);

    if (results.hasNext()) {
      Entity entity = results.next();
      return constructSchema(entity, config.isIncludeKey(), config.getKeyAlias());
    }
    throw new DatastoreExecutionException("Cloud Datastore query did not return any results. "
                                            + "Please check Namespace, Kind and Ancestor properties.");
  }

  /**
   * Constructs ancestor key using using given Datastore configuration.
   *
   * @param config Datastore configuration
   * @return Datastore key instance
   */
  private Key constructAncestorKey(DatastoreSourceConfig config) {
    List<PathElement> ancestor = config.getAncestor();

    if (ancestor.isEmpty()) {
      return null;
    }

    PathElement keyElement = ancestor.get(ancestor.size() - 1);
    Key.Builder keyBuilder;
    if (keyElement.hasId()) {
      keyBuilder = Key.newBuilder(config.getProject(), keyElement.getKind(), keyElement.getId());
    } else {
      keyBuilder = Key.newBuilder(config.getProject(), keyElement.getKind(), keyElement.getName());
    }

    if (ancestor.size() > 1) {
      ancestor.subList(0, ancestor.size() - 1).forEach(keyBuilder::addAncestor);
    }
    return keyBuilder.setNamespace(config.getNamespace()).build();
  }


  /**
   * Constructs CDAP schema based on given CDAP entity and source configuration,
   * will add Datastore key to the list of schema fields if config include key flag is set to true.
   *
   * @param entity Datastore entity
   * @param isIncludeKey flag that indicates that key should be included in schema
   * @param keyName key name
   * @return CDAP schema
   */
  @VisibleForTesting
  Schema constructSchema(Entity entity, boolean isIncludeKey, String keyName) {
    List<Schema.Field> fields = constructSchemaFields(entity);

    if (isIncludeKey) {
      fields.add(Schema.Field.of(keyName, Schema.of(Schema.Type.STRING)));
    }

    return Schema.recordOf("schema", fields);
  }

  /**
   * Constructs list of CDAP schema fields based on given Datastore entity,
   * filters out fields schemas with null value.
   *
   * @param entity Datastore entity
   * @return list of CDAP schema fields
   */
  private List<Schema.Field> constructSchemaFields(FullEntity<?> entity) {
    return entity.getNames().stream()
      .map(name -> transformToField(name, entity.getValue(name)))
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  /**
   * Since Datastore is schemaless database, returns nullable field schema for the given value type.
   * For {@link ValueType#NULL} will return default {@link Schema.Type#STRING},
   * for unsupported types will return null.
   *
   * @param name field name
   * @param value Datastore value type
   * @return CDAP field
   */
  private Schema.Field transformToField(String name, Value<?> value) {
    switch (value.getType()) {
      case STRING:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
      case LONG:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.LONG)));
      case DOUBLE:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.DOUBLE)));
      case BOOLEAN:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.BOOLEAN)));
      case TIMESTAMP:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)));
      case BLOB:
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.BYTES)));
      case NULL:
        // Datastore stores null values in Null Type unlike regular databases which have nullable type definition
        // set default String type, instead of skipping the field
        LOG.debug("Unable to determine type, setting default type '{}' for the field '{}'", Schema.Type.STRING, name);
        return Schema.Field.of(name, Schema.nullableOf(Schema.of(Schema.Type.STRING)));
      case ENTITY:
        List<Schema.Field> fields = constructSchemaFields(((EntityValue) value).get());
        return Schema.Field.of(name, Schema.nullableOf(Schema.recordOf(name, fields)));
      case LIST:
      case KEY:
      case LAT_LNG:
      case RAW_VALUE:
      default:
        LOG.debug("Field '{}' is of unsupported type '{}', skipping field from the schema", name, value.getType());
        return null;
    }
  }

}
