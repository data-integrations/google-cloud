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

package io.cdap.plugin.gcp.firestore.source;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import io.cdap.plugin.common.LineageRecorder;
import io.cdap.plugin.gcp.firestore.exception.FirestoreInitializationException;
import io.cdap.plugin.gcp.firestore.util.FirestoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.cdap.plugin.gcp.common.GCPConfig.NAME_PROJECT;
import static io.cdap.plugin.gcp.common.GCPConfig.NAME_SERVICE_ACCOUNT_FILE_PATH;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.ID_PROPERTY_NAME;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PLUGIN_NAME;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_COLLECTION;

/**
 * A {@link BatchSource} that reads data from Firestore and converts each document into
 * a {@link StructuredRecord} using the specified Schema.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name(PLUGIN_NAME)
@Description("Firestore Batch Source will read documents from Firestore and convert each document " +
  "into a StructuredRecord with the help of the specified Schema. ")
public class FirestoreSource extends BatchSource<Object, QueryDocumentSnapshot, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(FirestoreSource.class);

    /*
    private static final Map<Value.ValueTypeCase, Schema> SUPPORTED_SIMPLE_TYPES1 =
            new ImmutableMap.Builder<Value.ValueTypeCase, Schema>()
                    .put(NULL_VALUE, Schema.of(Schema.Type.NULL))
                    .put(BOOLEAN_VALUE, Schema.of(Schema.Type.BOOLEAN))
                    .put(INTEGER_VALUE, Schema.of(Schema.Type.INT))
                    .put(DOUBLE_VALUE, Schema.of(Schema.Type.DOUBLE))
                    .put(TIMESTAMP_VALUE, Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))
                    .put(STRING_VALUE, Schema.of(Schema.Type.STRING))
                    .put(BYTES_VALUE, Schema.of(Schema.Type.BYTES))
                    .build();
    */

  private static final Map<String, Schema> SUPPORTED_SIMPLE_TYPES =
    new ImmutableMap.Builder<String, Schema>()
      .put(Boolean.class.getName(), Schema.of(Schema.Type.BOOLEAN))
      .put(Integer.class.getName(), Schema.of(Schema.Type.INT))
      .put(Long.class.getName(), Schema.of(Schema.Type.LONG))
      .put(Double.class.getName(), Schema.of(Schema.Type.DOUBLE))
      .put(String.class.getName(), Schema.of(Schema.Type.STRING))
      .build();

    /*
    com.google.firestore.v1.Value.ValueTypeCase
    REFERENCE_VALUE(5),
    GEO_POINT_VALUE(8),
    ARRAY_VALUE(9),
    MAP_VALUE(6),
    VALUETYPE_NOT_SET(0);
    */

  private final FirestoreSourceConfig config;
  private QueryDocumentSnapshotToRecordTransformer queryDocumentSnapshotToRecordTransformer;

  public FirestoreSource(FirestoreSourceConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);

    LOG.debug("Validate config during `configurePipeline` stage: {}", config);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();

    config.validate(collector);
    // Since we have validated all the properties, throw an exception if there are any errors in the collector.
    // This is to avoid adding same validation errors again in getSchema method call
    collector.getOrThrowException();

    Schema configuredSchema = config.getSchema(collector);
    if (!config.shouldConnect()) {
      stageConfigurer.setOutputSchema(configuredSchema);
      return;
    }


    if (configuredSchema == null) {
      Schema schema = getSchema(collector);
      stageConfigurer.setOutputSchema(schema);
      return;
    }

    pipelineConfigurer.getStageConfigurer().setOutputSchema(configuredSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    FailureCollector collector = context.getFailureCollector();
    config.validate(collector);
    collector.getOrThrowException();

    String project = config.getProject();
    String serviceAccountFile = config.getServiceAccountFilePath();
    String database = config.getDatabase();
    String collection = config.getCollection();
    //String splits = String.valueOf(config.getNumSplits());
    String mode = config.getQueryMode(collector).getValue();
    String pullDocuments = config.getPullDocuments();
    String skipDocuments = config.getSkipDocuments();
    String filters = config.getFilters();

    List<String> fields = fetchSchemaFields(config.getSchema(collector));

    context.setInput(Input.of(config.getReferenceName(), new FirestoreInputFormatProvider(project, serviceAccountFile,
      database, collection, mode, pullDocuments, skipDocuments, filters, fields)));

    emitLineage(context);
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    FailureCollector collector = context.getFailureCollector();
    queryDocumentSnapshotToRecordTransformer = new QueryDocumentSnapshotToRecordTransformer(
      config.getSchema(collector), config.isIncludeDocumentId(), config.getIdAlias());
  }

  @Override
  public void transform(KeyValue<Object, QueryDocumentSnapshot> input, Emitter<StructuredRecord> emitter)
    throws Exception {
    QueryDocumentSnapshot queryDocumentSnapshot = input.getValue();
    try {
      emitter.emit(queryDocumentSnapshotToRecordTransformer.transform(queryDocumentSnapshot));
    } catch (Exception e) {
      throw new RuntimeException("Failed to process record", e);
    }
  }

  private List<String> fetchSchemaFields(Schema schema) {
    return schema.getFields().stream()
      .filter(f -> !f.getName().equals(ID_PROPERTY_NAME))
      .map(Schema.Field::getName)
      .collect(Collectors.toList());
  }

  private void emitLineage(BatchSourceContext context) {
    FailureCollector collector = context.getFailureCollector();
    LineageRecorder lineageRecorder = new LineageRecorder(context, config.getReferenceName());
    lineageRecorder.createExternalDataset(config.getSchema(collector));
    List<Schema.Field> fields = Objects.requireNonNull(config.getSchema(collector)).getFields();
    if (fields != null && !fields.isEmpty()) {
      lineageRecorder.recordRead("Read",
        String.format("Read from '%s' Firestore collection.", config.getCollection()),
        fields.stream().map(Schema.Field::getName).collect(Collectors.toList()));
    }
  }

  private Schema getSchema(FailureCollector collector) {
    LOG.debug("Executing query for `Get Schema`");

    List<QueryDocumentSnapshot> items = null;
    try {
      Firestore db = FirestoreUtil.getFirestore(config.getServiceAccountFilePath(), config.getProject(),
        config.getDatabase());
      ApiFuture<QuerySnapshot> query = db.collection(config.getCollection()).limit(1).get();
      QuerySnapshot querySnapshot = query.get();

      items = querySnapshot.getDocuments();

    } catch (FirestoreInitializationException e) {
      collector.addFailure(e.getMessage(), "Ensure properties like project, service account " +
        "file path, collection are correct.")
        .withConfigProperty(NAME_SERVICE_ACCOUNT_FILE_PATH)
        .withConfigProperty(NAME_PROJECT)
        .withConfigProperty(PROPERTY_COLLECTION)
        .withStacktrace(e.getStackTrace());
      throw collector.getOrThrowException();

    } catch (Exception e) {
      collector.addFailure("Unable to fetch data from Firestore: " + e.getMessage(), null)
        .withStacktrace(e.getStackTrace());
      throw collector.getOrThrowException();
    }

    if (items != null && !items.isEmpty()) {
      QueryDocumentSnapshot entity = items.get(0);
      return constructSchema(entity, config.isIncludeDocumentId(), config.getIdAlias());
    }

    collector.addFailure("Cloud Firestore query did not return any results. ",
      "Ensure Collection property is set correct.")
      .withConfigProperty(PROPERTY_COLLECTION);

    throw collector.getOrThrowException();
  }

  /**
   * Constructs CDAP schema based on given DocumentSnapshot and source configuration,
   * will add Firestore document id to the list of schema fields if config include key flag is set to true.
   *
   * @param entity      QueryDocumentSnapshot entity
   * @param isIncludeId flag that indicates that document id should be included in schema
   * @param idName      id name
   * @return CDAP schema
   */
  @VisibleForTesting
  Schema constructSchema(QueryDocumentSnapshot entity, boolean isIncludeId, String idName) {
    List<Schema.Field> fields = constructSchemaFields(entity);

    if (isIncludeId) {
      fields.add(0, Schema.Field.of(idName, Schema.of(Schema.Type.STRING)));
    }

    return Schema.recordOf("schema", fields);
  }

  /**
   * Constructs list of CDAP schema fields based on given Firestore document,
   * filters out fields schemas with null value.
   *
   * @param entity Firestore document
   * @return list of CDAP schema fields
   */
  private List<Schema.Field> constructSchemaFields(QueryDocumentSnapshot entity) {
    return entity.getData().keySet().stream()
      .map(name -> transformToField(name, entity.get(name)))
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  /**
   * Since Firestore is schemaless database, creates field with nullable schema for the given value
   * based on its value type, for unsupported types returns null.
   *
   * @param name  field name
   * @param value Firestore value
   * @return CDAP field
   */
  private Schema.Field transformToField(String name, Object value) {
    Schema schema = createSchema(name, value);
    if (schema == null) {
      return null;
    }
    return Schema.Type.NULL == schema.getType()
      ? Schema.Field.of(name, schema)
      : Schema.Field.of(name, Schema.nullableOf(schema));
  }

  /**
   * Creates CDAP schema based on given Firestore value and its type,
   * for unsupported types will return null.
   *
   * @param name  field name
   * @param value Firestore value
   * @return CDAP schema
   */
  private Schema createSchema(String name, Object value) {
    Schema schema = SUPPORTED_SIMPLE_TYPES.get(value.getClass().getName());
    if (schema == null) {
      schema = SUPPORTED_SIMPLE_TYPES.get(String.class.getName());
    }

    if (schema != null) {
      return schema;
    }

        /*
        switch (value.getType()) {
            case ENTITY:
                List<Schema.Field> fields = constructSchemaFields(((EntityValue) value).get());
                return Schema.recordOf(name, fields);
            case LIST:
                @SuppressWarnings("unchecked")
                List<? extends Value<?>> values = (List<? extends Value<?>>) value.get();
                Set<Schema> arraySchemas = new HashSet<>();
                for (Value<?> val : values) {
                    Schema valSchema = createSchema(name, val);
                    if (valSchema == null) {
                        return null;
                    }
                    arraySchemas.add(valSchema);
                }

                if (arraySchemas.isEmpty()) {
                    return Schema.arrayOf(Schema.of(Schema.Type.NULL));
                }

                if (arraySchemas.size() == 1) {
                    Schema componentSchema = arraySchemas.iterator().next();
                    return Schema.Type.NULL == componentSchema.getType()
                            ? Schema.arrayOf(componentSchema)
                            : Schema.arrayOf(Schema.nullableOf(componentSchema));
                }

                LOG.debug("Field '{}' has several schemas in array, add them as union of schemas "
                        + "plus {} schema for null values", name, Schema.Type.NULL);
                arraySchemas.add(Schema.of(Schema.Type.NULL));
                return Schema.arrayOf(Schema.unionOf(arraySchemas));
        }
        */

    LOG.debug("Field '{}' is of unsupported type '{}', skipping field from the schema", name,
      value.getClass().getName());
    return null;
  }
}
