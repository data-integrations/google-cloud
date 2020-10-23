/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.cloud.firestore.Firestore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
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
import io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants;
import io.cdap.plugin.gcp.firestore.source.util.SourceQueryMode;
import io.cdap.plugin.gcp.firestore.util.FirestoreConstants;
import io.cdap.plugin.gcp.firestore.util.FirestoreUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Defines a base {@link PluginConfig} that Firestore Source.
 */
public class FirestoreSourceConfig extends GCPReferenceSourceConfig {
  private static final Logger LOG = LoggerFactory.getLogger(FirestoreSourceConfig.class);

  @Name(FirestoreConstants.PROPERTY_DATABASE_ID)
  @Description("Firestore database name.")
  @Macro
  @Nullable
  private String database;

  @Name(FirestoreConstants.PROPERTY_COLLECTION)
  @Description("Name of the database collection.")
  @Macro
  private String collection;

  @Name(FirestoreSourceConstants.PROPERTY_INCLUDE_ID)
  @Description("A flag to specify document id to be included in output")
  @Macro
  private String includeDocumentId;

  @Name(FirestoreSourceConstants.PROPERTY_ID_ALIAS)
  @Description("Name of the field to set as the id field. This value is ignored if the `Include Document Id` is set to "
    + "`false`. If no value is provided, `__id__` is used.")
  @Macro
  @Nullable
  private String idAlias;

  @Name(FirestoreSourceConstants.PROPERTY_QUERY_MODE)
  @Macro
  @Description("Mode of query. The mode can be one of two values: "
    + "`Basic` - will allow user to specify documents to pull or skip, `Advanced` - will allow user to "
    + "specify custom query.")
  private String queryMode;

  @Name(FirestoreSourceConstants.PROPERTY_PULL_DOCUMENTS)
  @Macro
  @Nullable
  @Description("Specify the document ids to be extracted from Firestore Collection; for example: 'Doc1,Doc2'.")
  private String pullDocuments;

  @Name(FirestoreSourceConstants.PROPERTY_SKIP_DOCUMENTS)
  @Macro
  @Nullable
  @Description("Specify the document ids to be skipped from Firestore Collection; for example: 'Doc1,Doc2'.")
  private String skipDocuments;

  @Name(FirestoreSourceConstants.PROPERTY_CUSTOM_QUERY)
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

  @Name(FirestoreSourceConstants.PROPERTY_SCHEMA)
  @Description("Schema of records output by the source.")
  private String schema;

  /**
   * Constructor for FirestoreSourceConfig object.
   * @param referenceName the reference name
   * @param project the project id
   * @param serviceFilePath the service file path
   * @param database the database id
   * @param collection the collection id
   * @param queryMode the query mode (basic or advanced)
   * @param pullDocuments the list of documents to pull
   * @param skipDocuments the list of documents to skip
   * @param filters the filter for given field as well as value
   * @param includeDocumentId the included document id
   * @param idAlias the id alias
   * @param schema the schema
   */
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

  /**
   * Returns the query mode chosen.
   *
   * @param collector The failure collector to collect the errors
   * @return An instance of SourceQueryMode
   */
  public SourceQueryMode getQueryMode(FailureCollector collector) {
    SourceQueryMode mode = getQueryMode();
    if (mode != null) {
      return mode;
    }

    collector.addFailure("Unsupported query mode value: " + queryMode,
                         String.format("Supported modes are: %s", SourceQueryMode.getSupportedModes()))
      .withConfigProperty(FirestoreSourceConstants.PROPERTY_QUERY_MODE);
    collector.getOrThrowException();
    return null;
  }

  /**
   * Returns the query mode chosen.
   *
   * @return An instance of SourceQueryMode
   */
  public SourceQueryMode getQueryMode() {
    Optional<SourceQueryMode> sourceQueryMode = SourceQueryMode.fromValue(queryMode);

    return sourceQueryMode.isPresent() ? sourceQueryMode.get() : null;
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

  /**
   * Return the Schema.
   * @param collector the FailureCollector
   * @return The Schema
   */
  public Schema getSchema(FailureCollector collector) {
    if (Strings.isNullOrEmpty(schema)) {
      return null;
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      collector.addFailure("Invalid schema: " + e.getMessage(), null)
        .withConfigProperty(FirestoreSourceConstants.PROPERTY_SCHEMA);
      // if there was an error that was added, it will throw an exception, otherwise, this statement will
      // not be executed
      collector.getOrThrowException();
      return null;
    }
  }

  /**
   * Validates {@link FirestoreSourceConfig} instance.
   */
  public void validate(FailureCollector collector) {
    super.validate(collector);
    validateFirestoreConnection(collector);
    validateCollection(collector);
    validateDocumentLists(collector);
    validateFilters(collector);

    if (containsMacro(FirestoreSourceConstants.PROPERTY_SCHEMA)) {
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
        .withConfigProperty(FirestoreConstants.PROPERTY_COLLECTION)
        .withStacktrace(e.getStackTrace());
    } catch (Exception e) {
      LOG.error("Error", e);
    }
  }

  /**
   * Validates the given referenceName to consists of characters allowed to represent a dataset.
   */
  public void validateCollection(FailureCollector collector) {
    if (containsMacro(FirestoreConstants.PROPERTY_COLLECTION)) {
      return;
    }

    if (Strings.isNullOrEmpty(getCollection())) {
      collector.addFailure("Collection must be specified.", null)
        .withConfigProperty(FirestoreConstants.PROPERTY_COLLECTION);
    }
  }

  private void validateSchema(Schema schema, FailureCollector collector) {
    List<Schema.Field> fields = schema.getFields();
    if (fields == null || fields.isEmpty()) {
      collector.addFailure("Source schema must contain at least one field", null)
        .withConfigProperty(FirestoreSourceConstants.PROPERTY_SCHEMA);
    } else {
      fields.forEach(f -> validateFieldSchema(f.getName(), f.getSchema(), collector));
    }
  }

  /**
   * Validates given field schema to be compliant with Firestore types.
   *
   * @param fieldName field name
   * @param fieldSchema schema for CDAP field
   * @param collector failure collector to collect failures if schema contains unsupported type.
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
    return !containsMacro(FirestoreSourceConstants.PROPERTY_SCHEMA) &&
      !containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) &&
      !containsMacro(NAME_PROJECT) &&
      tryGetProject() != null &&
      !autoServiceAccountUnavailable();
  }
  
  private void validateDocumentLists(FailureCollector collector) {
    if (containsMacro(FirestoreSourceConstants.PROPERTY_PULL_DOCUMENTS) ||
      containsMacro(FirestoreSourceConstants.PROPERTY_SKIP_DOCUMENTS)) {
      return;
    }

    SourceQueryMode mode = getQueryMode(collector);

    List<String> pullDocumentList = Splitter.on(',').trimResults().splitToList(getPullDocuments());
    List<String> skipDocumentList = Splitter.on(',').trimResults().splitToList(getSkipDocuments());

    if (mode == SourceQueryMode.BASIC) {
      if (!pullDocumentList.isEmpty() && !skipDocumentList.isEmpty()) {
        collector.addFailure("Either Documents to pull Or Documents to skip should be defined", null)
          .withConfigProperty(FirestoreSourceConstants.PROPERTY_PULL_DOCUMENTS)
          .withConfigProperty(FirestoreSourceConstants.PROPERTY_SKIP_DOCUMENTS);
      }
    } else if (mode == SourceQueryMode.ADVANCED) {
      if (!pullDocumentList.isEmpty() || !skipDocumentList.isEmpty()) {
        collector.addFailure("In case of Mode=Advanced, Both Documents to pull Or Documents to skip " +
          "must be empty", null)
          .withConfigProperty(FirestoreSourceConstants.PROPERTY_PULL_DOCUMENTS)
          .withConfigProperty(FirestoreSourceConstants.PROPERTY_SKIP_DOCUMENTS);
      }
    }
  }

  private void validateFilters(FailureCollector collector) {
    if (containsMacro(FirestoreSourceConstants.PROPERTY_CUSTOM_QUERY)) {
      return;
    }

    SourceQueryMode mode = getQueryMode(collector);

    if (mode == SourceQueryMode.BASIC && !Strings.isNullOrEmpty(getFilters())) {
      collector.addFailure("In case of Mode=Basic, Filters must be empty", null)
        .withConfigProperty(FirestoreSourceConstants.PROPERTY_CUSTOM_QUERY);
    } else if (mode == SourceQueryMode.ADVANCED) {
      List<FilterInfo> filters = getFiltersAsList(collector);
      collector.getOrThrowException();
      if (filters.isEmpty()) {
        collector.addFailure("In case of Mode=Advanced, Filters must contain at least one filter", null)
          .withConfigProperty(FirestoreSourceConstants.PROPERTY_CUSTOM_QUERY);
        return;
      }
    }
  }

  /**
   * Returns the empty list if filters contains a macro. Otherwise, the list returned can never be empty.
   * @param collector the FailureCollector
   * @return the this of FilterInfo
   */
  public List<FilterInfo> getFiltersAsList(FailureCollector collector) {
    if (containsMacro(FirestoreSourceConstants.PROPERTY_CUSTOM_QUERY)) {
      return Collections.emptyList();
    }

    try {
      List<FilterInfo> filterInfos = FilterInfoParser.parseFilterString(filters);
      return filterInfos;
    } catch (Exception e) {
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(FirestoreSourceConstants.PROPERTY_CUSTOM_QUERY);
      return Collections.emptyList();
    }
  }
}
