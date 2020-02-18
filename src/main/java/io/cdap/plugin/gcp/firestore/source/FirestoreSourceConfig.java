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

import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.Firestore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;
import io.cdap.plugin.gcp.firestore.exception.FirestoreInitializationException;
import io.cdap.plugin.gcp.firestore.source.util.FilterInfo;
import io.cdap.plugin.gcp.firestore.source.util.FilterInfoParser;
import io.cdap.plugin.gcp.firestore.source.util.SourceQueryMode;
import io.cdap.plugin.gcp.firestore.util.FirestoreUtil;
import io.cdap.plugin.gcp.firestore.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;

import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_CUSTOM_QUERY;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_ID_ALIAS;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_INCLUDE_ID;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_PULL_DOCUMENTS;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_QUERY_MODE;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_SCHEMA;
import static io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants.PROPERTY_SKIP_DOCUMENTS;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_COLLECTION;
import static io.cdap.plugin.gcp.firestore.util.FirestoreConstants.PROPERTY_DATABASE_ID;

/**
 * Defines a base {@link PluginConfig} that Firestore Source and Sink can re-use.
 */
public class FirestoreSourceConfig extends GCPReferenceSourceConfig {
  public static final Schema ERROR_SCHEMA = Schema.recordOf("error", Schema.Field.of("document",
    Schema.of(Schema.Type.STRING)));
  public static final Set<Schema.Type> SUPPORTED_SIMPLE_TYPES = ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.INT,
    Schema.Type.DOUBLE, Schema.Type.BYTES,
    Schema.Type.LONG, Schema.Type.STRING,
    Schema.Type.ARRAY, Schema.Type.RECORD,
    Schema.Type.MAP);
  public static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES = ImmutableSet.of(
    Schema.LogicalType.DECIMAL, Schema.LogicalType.TIMESTAMP_MILLIS, Schema.LogicalType.TIMESTAMP_MICROS);
  private static final Logger LOG = LoggerFactory.getLogger(FirestoreSourceConfig.class);

  /*
private static final Map<ValueType, Schema> SUPPORTED_SIMPLE_TYPES = new ImmutableMap.Builder<ValueType, Schema>()
        .put(ValueType.STRING, Schema.of(Schema.Type.STRING))
        .put(ValueType.LONG, Schema.of(Schema.Type.LONG))
        .put(ValueType.DOUBLE, Schema.of(Schema.Type.DOUBLE))
        .put(ValueType.BOOLEAN, Schema.of(Schema.Type.BOOLEAN))
        .put(ValueType.TIMESTAMP, Schema.of(Schema.LogicalType.TIMESTAMP_MICROS))
        .put(ValueType.BLOB, Schema.of(Schema.Type.BYTES))
        .put(ValueType.NULL, Schema.of(Schema.Type.NULL))
        .build();
*/
  @Name(PROPERTY_DATABASE_ID)
  @Description("Firestore database name.")
  @Macro
  @Nullable
  private String database;

  @Name(PROPERTY_COLLECTION)
  @Description("Name of the database collection.")
  @Macro
  private String collection;

  @Name(PROPERTY_INCLUDE_ID)
  @Description("A flag to specify document id to be included in output")
  @Macro
  private String includeDocumentId;

  @Name(PROPERTY_ID_ALIAS)
  @Description("Name of the field to set as the id field. This value is ignored if the `Include Document Id` is set to "
    + "`false`. If no value is provided, `__id__` is used.")
  @Macro
  @Nullable
  private String idAlias;

  /*
  @Name(PROPERTY_NUM_SPLITS)
  @Macro
  @Description("Desired number of splits to divide the query into when reading from Cloud Datastore. "
    + "Fewer splits may be created if the query cannot be divided into the desired number of splits.")
  private int numSplits;
  */

  @Name(PROPERTY_QUERY_MODE)
  @Macro
  @Description("Mode of query. The mode can be one of two values: "
    + "`Basic` - will allow user to specify documents to pull or skip, `Advanced` - will allow user to "
    + "specify custom query.")
  private String queryMode;

  @Name(PROPERTY_PULL_DOCUMENTS)
  @Macro
  @Nullable
  @Description("Specify the document ids to be extracted from Firestore Collection; for example: 'Doc1,Doc2'.")
  private String pullDocuments;

  @Name(PROPERTY_SKIP_DOCUMENTS)
  @Macro
  @Nullable
  @Description("Specify the document ids to be skipped from Firestore Collection; for example: 'Doc1,Doc2'.")
  private String skipDocuments;

  @Name(PROPERTY_CUSTOM_QUERY)
  @Macro
  @Nullable
  @Description("Specify the custom filter for fetching documents from Firestore Collection. " +
    "Supported operators are, EqualTo, NumericEqualTo, LessThan, LessThanOrEqualTo, GreaterThan, " +
    "GreaterThanOrEqualTo. A filter must specify the operator with field it should filter on as well the value. " +
    "Filters are specified using syntax: \"value:operator(field)[,value:operator(field)]\". " +
    "For example, 'CA:EqualTo(state),1000000:LessThan(population)' will apply two filters. " +
    "The first will create a filter as state = 'CA'." +
    "The second will create a filter as population < 1000000.")
  private String filters;

  @Name(PROPERTY_SCHEMA)
  @Description("Schema of records output by the source.")
  private String schema;

  public FirestoreSourceConfig(String referenceName, String project, String serviceFilePath, String database,
                               String collection, String queryMode, String pullDocuments, String skipDocuments,
                               String filters, String includeDocumentId, String idAlias, String schema) {
    this.referenceName = referenceName;
    this.project = project;
    this.serviceFilePath = serviceFilePath;
    this.database = database;
    this.collection = collection;
    this.queryMode = queryMode;
    this.pullDocuments = pullDocuments;
    this.skipDocuments = skipDocuments;
    this.filters = filters;
    this.includeDocumentId = includeDocumentId;
    this.idAlias = idAlias;
    this.schema = schema;
  }

  public String getReferenceName() {
    return referenceName;
  }

  @Nullable
  public String getDatabase() {
    return database;
  }

  public String getCollection() {
    return collection;
  }

  public SourceQueryMode getQueryMode(FailureCollector collector) {
    Optional<SourceQueryMode> sourceQueryMode = SourceQueryMode.fromValue(queryMode);
    if (sourceQueryMode.isPresent()) {
      return sourceQueryMode.get();
    }
    collector.addFailure("Unsupported query mode value: " + queryMode,
      String.format("Supported modes are: %s", SourceQueryMode.getSupportedModes()))
      .withConfigProperty(PROPERTY_QUERY_MODE);
    throw collector.getOrThrowException();
  }

  @Nullable
  public String getPullDocuments() {
    return pullDocuments;
  }

  @Nullable
  public String getSkipDocuments() {
    return skipDocuments;
  }

  @Nullable
  public String getFilters() {
    return filters;
  }

  public boolean isIncludeDocumentId() {
    return includeDocumentId != null && includeDocumentId.equalsIgnoreCase("true");
  }

  @Nullable
  public String getIdAlias() {
    return idAlias;
  }

  public Schema getSchema(FailureCollector collector) {
    if (Util.isNullOrEmpty(schema)) {
      return null;
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(), null)
        .withConfigProperty(PROPERTY_SCHEMA);
    }
    // if there was an error that was added, it will throw an exception, otherwise, this statement will not be executed
    throw collector.getOrThrowException();
  }

  /**
   * Validates {@link FirestoreSourceConfig} instance.
   */
  public void validate(FailureCollector collector) {
    super.validate(collector);
    validateFirestoreConnection(collector);
    validateCollection(collector);
    //validateNumSplits(collector);
    validateDocumentLists(collector);
    validateFilters(collector);

    if (containsMacro(PROPERTY_SCHEMA)) {
      return;
    }

    Schema schema = getSchema(collector);
    if (schema != null) {
      validateSchema(schema, collector);
    }
  }

  @VisibleForTesting
  void validateFirestoreConnection(FailureCollector collector) {
    if (!shouldConnect()) {
      return;
    }
    Firestore db = null;
    try {
      db = FirestoreUtil.getFirestore(getServiceAccountFilePath(), getProject(), getDatabase());

      if (db != null) {
        //checkCollectionExists(db);
        db.close();
      }
    } catch (FirestoreInitializationException e) {
      collector.addFailure(e.getMessage(), "Ensure properties like project, service account " +
        "file path are correct.")
        .withConfigProperty(NAME_SERVICE_ACCOUNT_FILE_PATH)
        .withConfigProperty(NAME_PROJECT)
        .withStacktrace(e.getStackTrace());
    } catch (IllegalArgumentException e) {
      collector.addFailure(e.getMessage(), "Ensure collection name exists in Firestore.")
        .withConfigProperty(PROPERTY_COLLECTION)
        .withStacktrace(e.getStackTrace());
    } catch (Exception e) {
      LOG.error("Error", e);
    }
  }

  public void validateCollection(FailureCollector collector) {
    if (containsMacro(PROPERTY_COLLECTION)) {
      return;
    }

    if (Util.isNullOrEmpty(getCollection())) {
      collector.addFailure("Collection must be specified.", null)
        .withConfigProperty(PROPERTY_COLLECTION);
    }
  }

  private void checkCollectionExists(Firestore db) throws IllegalArgumentException {
    if (containsMacro(PROPERTY_COLLECTION)) {
      return;
    }

    String collectionName = Strings.nullToEmpty(getCollection()).trim();

    List<String> collections = StreamSupport.stream(db.listCollections().spliterator(), false)
      .map(CollectionReference::getId).collect(Collectors.toList());
    if (!collections.contains(collectionName)) {
      throw new IllegalArgumentException("Invalid collection");
    }
  }

  private void validateSchema(Schema schema, FailureCollector collector) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      collector.addFailure("Source schema must contain at least one field", null)
        .withConfigProperty(PROPERTY_SCHEMA);
    } else {
      fields.forEach(f -> validateFieldSchema(f.getName(), f.getSchema(), collector));
    }
  }

  /**
   * Validates given field schema to be compliant with Firestore types.
   *
   * @param fieldName   field name
   * @param fieldSchema schema for CDAP field
   * @param collector   failure collector to collect failures if schema contains unsupported type.
   */
  private void validateFieldSchema(String fieldName, Schema fieldSchema, FailureCollector collector) {
    Schema.LogicalType logicalType = fieldSchema.getLogicalType();
    if (logicalType != null) {
      // timestamps in CDAP are represented as LONG with TIMESTAMP_MICROS logical type
      if (logicalType != Schema.LogicalType.TIMESTAMP_MICROS) {
        collector.addFailure(String.format("Field '%s' is of unsupported type '%s'",
          fieldName, fieldSchema.getDisplayName()),
          "Supported types are: string, double, boolean, bytes, long, record, " +
            "array, union and timestamp.")
          .withOutputSchemaField(fieldName);
        return;
      }
    }

    switch (fieldSchema.getType()) {
      case STRING:
      case DOUBLE:
      case BOOLEAN:
      case BYTES:
      case LONG:
      case NULL:
        return;
      case RECORD:
        validateSchema(fieldSchema, collector);
        return;
      case ARRAY:
        if (fieldSchema.getComponentSchema() == null) {
          collector.addFailure(String.format("Field '%s' has no schema for array type", fieldName),
            "Ensure array component has schema.").withOutputSchemaField(fieldName);
          return;
        }

        Schema componentSchema = fieldSchema.getComponentSchema();
        if (Schema.Type.ARRAY == componentSchema.getType()) {
          collector.addFailure(String.format("Field '%s' is of unsupported type array of array.", fieldName),
            "Ensure the field has valid type.")
            .withOutputSchemaField(fieldName);
          return;
        }
        validateFieldSchema(fieldName, componentSchema, collector);

        return;
      case UNION:
        fieldSchema.getUnionSchemas().forEach(unionSchema ->
          validateFieldSchema(fieldName, unionSchema, collector));
        return;
      default:
        collector.addFailure(String.format("Field '%s' is of unsupported type '%s'",
          fieldName, fieldSchema.getDisplayName()),
          "Supported types are: string, double, boolean, bytes, long, record, " +
            "array, union and timestamp.")
          .withOutputSchemaField(fieldName);
    }
  }

  /**
   * Returns true if Firestore can be connected to or schema is not a macro.
   */
  public boolean shouldConnect() {
    return !containsMacro(PROPERTY_SCHEMA) &&
      !containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) &&
      !containsMacro(NAME_PROJECT) &&
      tryGetProject() != null &&
      !autoServiceAccountUnavailable();
  }

  /*
  private void validateNumSplits(FailureCollector collector) {
    if (containsMacro(PROPERTY_NUM_SPLITS)) {
      return;
    }

    if (numSplits < 1) {
      collector.addFailure("Number of splits must be greater than 0", null)
        .withConfigProperty(PROPERTY_NUM_SPLITS);
    }
  }
  */

  private void validateDocumentLists(FailureCollector collector) {
    if (containsMacro(PROPERTY_PULL_DOCUMENTS) || containsMacro(PROPERTY_SKIP_DOCUMENTS)) {
      return;
    }

    SourceQueryMode mode = getQueryMode(collector);

    List<String> pullDocumentList = Util.splitToList(getPullDocuments(), ',');
    List<String> skipDocumentList = Util.splitToList(getSkipDocuments(), ',');

    if (mode == SourceQueryMode.BASIC) {
      if (!pullDocumentList.isEmpty() && !skipDocumentList.isEmpty()) {
        collector.addFailure("Either Documents to pull Or Documents to skip should be defined", null)
          .withConfigProperty(PROPERTY_PULL_DOCUMENTS)
          .withConfigProperty(PROPERTY_SKIP_DOCUMENTS);
      }
    } else if (mode == SourceQueryMode.ADVANCED) {
      if (!pullDocumentList.isEmpty() || !skipDocumentList.isEmpty()) {
        collector.addFailure("In case of Mode=Advanced, Both Documents to pull Or Documents to skip " +
          "must be empty", null)
          .withConfigProperty(PROPERTY_PULL_DOCUMENTS)
          .withConfigProperty(PROPERTY_SKIP_DOCUMENTS);
      }
    }
  }

  private void validateFilters(FailureCollector collector) {
    if (containsMacro(PROPERTY_CUSTOM_QUERY)) {
      return;
    }

    //2020:Less Than(born)
    SourceQueryMode mode = getQueryMode(collector);

    if (mode == SourceQueryMode.BASIC && !Util.isNullOrEmpty(getFilters())) {
      collector.addFailure("In case of Mode=Basic, Filters must be empty", null)
        .withConfigProperty(PROPERTY_CUSTOM_QUERY);
    } else if (mode == SourceQueryMode.ADVANCED) {
      List<FilterInfo> filters = getFiltersAsList(collector);
      collector.getOrThrowException();
      if (filters.isEmpty()) {
        collector.addFailure("In case of Mode=Advanced, Filters must contain at least one filter", null)
          .withConfigProperty(PROPERTY_CUSTOM_QUERY);
        return;
      }
    }
  }

  /**
   * @return the filters to apply. Returns an empty list if filters contains a macro. Otherwise, the list
   * returned can never be empty.
   */
  public List<FilterInfo> getFiltersAsList(FailureCollector collector) {
    if (containsMacro(PROPERTY_CUSTOM_QUERY)) {
      return Collections.emptyList();
    }

    try {
      List<FilterInfo> filterInfos = FilterInfoParser.parseFilterString(filters);
      return filterInfos;
    } catch (Exception e) {
      collector.addFailure(e.getMessage(), null).withConfigProperty(PROPERTY_CUSTOM_QUERY);
      return Collections.emptyList();
    }
  }
}
