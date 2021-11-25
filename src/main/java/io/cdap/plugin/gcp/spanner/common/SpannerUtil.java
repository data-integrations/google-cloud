/*
 * Copyright © 2018 Cask Data, Inc.
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

package io.cdap.plugin.gcp.spanner.common;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.spi.v1.SpannerInterceptorProvider;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.GeneratedMessageV3;
import com.google.spanner.v1.CommitRequest;
import com.google.spanner.v1.Mutation;
import com.google.spanner.v1.PartialResultSet;
import com.google.spanner.v1.ResultSet;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.spanner.SpannerArrayConstants;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Spanner utility class to get spanner service
 */
public class SpannerUtil {
  private static final Set<Schema.LogicalType> SUPPORTED_LOGICAL_TYPES =
    ImmutableSet.of(Schema.LogicalType.DATE, Schema.LogicalType.TIMESTAMP_MICROS, Schema.LogicalType.DATETIME);
  private static final String TABLE_NAME = "TableName";
  // listing table's schema documented at https://cloud.google.com/spanner/docs/information-schema
  private static final Statement.Builder SCHEMA_STATEMENT_BUILDER = Statement.newBuilder(
    String.format("SELECT  t.column_name,t.spanner_type, t.is_nullable FROM information_schema.columns AS t WHERE " +
                    "  t.table_catalog = ''  AND  t.table_schema = '' AND t.table_name = @%s", TABLE_NAME));


  /**
   * Construct and return the {@link Spanner} service for the provided credentials and projectId
   */
  public static Spanner getSpannerService(String serviceAccount, boolean isServiceAccountFilePath, String projectId)
    throws IOException {
    SpannerOptions.Builder optionsBuilder = buildSpannerOptions(serviceAccount, isServiceAccountFilePath, projectId);
    return optionsBuilder.build().getService();
  }

  public static Spanner getSpannerService(GCPConnectorConfig config) throws IOException {
    return getSpannerService(config.getServiceAccount(), config.isServiceAccountFilePath(), config.getProject());
  }

  /**
   * Construct and return the {@link Spanner} service with an interceptor that increments the provided counter by the
   * number of bytes read from Spanner.
   */
    public static Spanner getSpannerServiceWithReadInterceptor(String serviceAccount,
                                                               boolean isServiceAccountFilePath,
                                                               String projectId,
                                                               BytesCounter counter) throws IOException {
      class InterceptedClientCall<ReqT, RespT> extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {
        InterceptedClientCall(ClientCall<ReqT, RespT> call) {
          super(call);
        }

        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          super.start(new ForwardingClientCallListener.
            SimpleForwardingClientCallListener<RespT>(responseListener) {
            @Override
            public void onMessage(RespT message) {
              if (message instanceof PartialResultSet) {
                PartialResultSet partialResultSet = (PartialResultSet) message;
                counter.increment(partialResultSet.getSerializedSize());
              } else if (message instanceof ResultSet) {
                ResultSet resultSet = (ResultSet) message;
                counter.increment(resultSet.getSerializedSize());
              }
              super.onMessage(message);
            }
          }, headers);
        }
      }

      SpannerOptions.Builder optionsBuilder = buildSpannerOptions(serviceAccount, isServiceAccountFilePath, projectId);
      optionsBuilder.setInterceptorProvider(
      SpannerInterceptorProvider.createDefault()
      .with(
        new ClientInterceptor() {
          @Override
          public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
            return new InterceptedClientCall<>(call);
          }
        }
      ));

    return optionsBuilder.build().getService();
  }

  /**
   * Construct and return the {@link Spanner} service with an interceptor that increments the provided counter by the
   * number of bytes written to Spanner.
   */
  public static Spanner getSpannerServiceWithWriteInterceptor(String serviceAccount,
                                                              boolean isServiceAccountFilePath,
                                                              String projectId,
                                                              BytesCounter counter) throws IOException {
    class InterceptedClientCall<ReqT, RespT> extends ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT> {
      InterceptedClientCall(ClientCall<ReqT, RespT> call) {
        super(call);
      }
      @Override
      public void sendMessage(ReqT message) {
        if (message instanceof CommitRequest) {
          // Increment counter by the total size of write mutation protos written to Spanner
          CommitRequest commitRequest = (CommitRequest) message;
          commitRequest.getMutationsList().stream()
            .map(GeneratedMessageV3::getAllFields)
            .map(Map::values)
            .flatMap(Collection::stream)
            .filter(Mutation.Write.class::isInstance)
            .map(Mutation.Write.class::cast)
            .map(Mutation.Write::getSerializedSize)
            .forEach(counter::increment);
        }
        super.sendMessage(message);
      }
    }

    SpannerOptions.Builder optionsBuilder = buildSpannerOptions(serviceAccount, isServiceAccountFilePath, projectId);
    optionsBuilder.setInterceptorProvider(
      SpannerInterceptorProvider.createDefault()
        .with(
          new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
              MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
              ClientCall<ReqT, RespT> call = next.newCall(method, callOptions);
              return new InterceptedClientCall<>(call);
            }
          }
        ));

    return optionsBuilder.build().getService();
  }

    /**
     * Construct and return a {@link SpannerOptions.Builder} with the provided credentials and projectId
     */
  private static SpannerOptions.Builder buildSpannerOptions(String serviceAccount,
                                                            boolean isServiceAccountFilePath,
                                                            String projectId) throws IOException {
    SpannerOptions.Builder optionsBuilder = SpannerOptions.newBuilder();
    if (serviceAccount != null) {
      optionsBuilder.setCredentials(GCPUtils.loadServiceAccountCredentials(serviceAccount, isServiceAccountFilePath));
    }
    optionsBuilder.setProjectId(projectId);
    return optionsBuilder;
  }

  /**
   * Validate that the schema is a supported one, compatible with Spanner.
   */
  public static void validateSchema(Schema schema, Set<Schema.Type> supportedTypes, FailureCollector collector) {
    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;

      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType != null && !SUPPORTED_LOGICAL_TYPES.contains(logicalType)) {
        collector.addFailure(
          String.format("Field '%s' is of unsupported type '%s'.", field.getName(), fieldSchema.getDisplayName()),
          "Change the type to be a date, timestamp or datetime.").withOutputSchemaField(field.getName());
      }

      if (logicalType == null) {
        Schema.Type type = fieldSchema.getType();
        if (!supportedTypes.contains(type)) {
          collector.addFailure(
            String.format("Field '%s' is of unsupported type '%s'.", field.getName(), fieldSchema.getDisplayName()),
            String.format("Supported types are: %s, date, datetime and timestamp.",
                          supportedTypes.stream().map(t -> t.name().toLowerCase()).collect(Collectors.joining(", "))))
            .withOutputSchemaField(field.getName());
        }
      }
    }
  }

  /**
   * Converts schema to Spanner create statement
   *
   * @param tableName table name to be created
   * @param primaryKeys comma separated list of primary keys
   * @param schema schema of the table
   * @return Create statement
   */
  public static String convertSchemaToCreateStatement(String tableName, String primaryKeys, Schema schema) {
    StringBuilder createStmt = new StringBuilder();
    createStmt.append("CREATE TABLE ").append(tableName).append(" (");

    for (Schema.Field field : schema.getFields()) {
      String name = field.getName();
      Schema fieldSchema = field.getSchema();
      fieldSchema = fieldSchema.isNullable() ? fieldSchema.getNonNullable() : fieldSchema;
      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      String spannerType;

      if (logicalType != null) {
        switch (logicalType) {
          case DATE:
            spannerType = "DATE";
            break;
          case TIMESTAMP_MILLIS:
          case TIMESTAMP_MICROS:
            spannerType = "TIMESTAMP";
            break;
          case DATETIME:
            spannerType = "STRING(MAX)";
            break;
          default:
            // this should not happen
            throw new IllegalStateException("Logical type '" + logicalType + "' is not supported");
        }
        addColumn(createStmt, name, field.getSchema().isNullable(), spannerType);
        continue;
      }

      Schema.Type type = fieldSchema.getType();
      switch (type) {
        case BOOLEAN:
          spannerType = "BOOL";
          break;
        case STRING:
          spannerType = "STRING(MAX)";
          break;
        case INT:
        case LONG:
          spannerType = "INT64";
          break;
        case FLOAT:
        case DOUBLE:
          spannerType = "FLOAT64";
          break;
        case BYTES:
          spannerType = "BYTES(MAX)";
          break;
        case ARRAY:
          Schema componentSchema = fieldSchema.getComponentSchema();
          if (componentSchema == null) {
            throw new IllegalStateException("Component schema of field '" + name + "' is null");
          }
          spannerType = getArrayType(componentSchema);
          break;
        default:
          throw new IllegalStateException(type.name() + " : Type currently not supported.");
      }
      addColumn(createStmt, name, field.getSchema().isNullable(), spannerType);
    }

    // remove trailing ", "
    createStmt.deleteCharAt(createStmt.length() - 1)
      .deleteCharAt(createStmt.length() - 1)
      .append(")");

    createStmt.append(" PRIMARY KEY (").append(primaryKeys).append(")");

    return createStmt.toString();
  }

  private static String getArrayType(Schema schema) {
    Schema componentSchema = schema.isNullable() ? schema.getNonNullable() : schema;
    Schema.LogicalType logicalType = componentSchema.getLogicalType();

    if (logicalType != null) {
      switch (logicalType) {
        case DATE:
          return "ARRAY<DATE>";
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return "ARRAY<TIMESTAMP>";
        default:
          // this should not happen
          throw new IllegalStateException("Array of '" + logicalType + "' logical type currently not supported.");
      }
    }

    Schema.Type type = componentSchema.getType();
    switch (type) {
      case BOOLEAN:
        return "ARRAY<BOOL>";
      case STRING:
        return "ARRAY<STRING(MAX)>";
      case INT:
      case LONG:
        return "ARRAY<INT64>";
      case FLOAT:
      case DOUBLE:
        return "ARRAY<FLOAT64>";
      case BYTES:
        return "ARRAY<BYTES(MAX)>";
      default:
        throw new IllegalStateException("Array of '" + type.name() + "' type currently not supported.");
    }
  }

  /**
   * Add column to create statement with appropriate type.
   */
  private static void addColumn(StringBuilder createStmt, String name, boolean isNullable, String spannerType) {
    createStmt.append(name).append(" ").append(spannerType);
    if (!isNullable) {
      createStmt.append(" NOT NULL");
    }
    createStmt.append(", ");
  }

  /**
   * Returns the schema of a Cloud Spanner talbe
   *
   * @param spanner Cloud Spanner instance
   * @param projectId Google Cloud project id
   * @param instance Cloud Spanner instance id
   * @param database Cloud Spanner database name
   * @param table Cloud Spanner table name
   * @param collector failure collector
   * @return Schema of the table
   */
  public static Schema getTableSchema(Spanner spanner, String projectId, String instance, String database, String table,
                                      FailureCollector collector) {
    Statement getTableSchemaStatement = SCHEMA_STATEMENT_BUILDER.bind(TABLE_NAME).to(table).build();
    DatabaseClient databaseClient =
      spanner.getDatabaseClient(DatabaseId.of(projectId, instance, database));
    try (com.google.cloud.spanner.ResultSet resultSet =
           databaseClient.singleUse().executeQuery(getTableSchemaStatement)) {
      List<Schema.Field> schemaFields = new ArrayList<>();
      while (resultSet.next()) {
        String columnName = resultSet.getString("column_name");
        String spannerType = resultSet.getString("spanner_type");
        String nullable = resultSet.getString("is_nullable");
        boolean isNullable = "YES".equals(nullable);
        Schema typeSchema = parseSchemaFromSpannerTypeString(columnName, spannerType, collector);
        if (typeSchema == null) {
          // this means there were failures added to failure collector. Continue to collect more failures
          continue;
        }
        Schema fieldSchema = isNullable ? Schema.nullableOf(typeSchema) : typeSchema;
        schemaFields.add(Schema.Field.of(columnName, fieldSchema));
      }
      if (schemaFields.isEmpty() && !collector.getValidationFailures().isEmpty()) {
        collector.getOrThrowException();
      }
      return Schema.recordOf("outputSchema", schemaFields);
    }
  }

  private static @Nullable Schema parseSchemaFromSpannerTypeString(String columnName, String spannerType,
                                                                   FailureCollector collector) {
    if (spannerType.startsWith("ARRAY")) {
      if (spannerType.startsWith(SpannerArrayConstants.ARRAY_STRING_PREFIX)) {
        return Schema.arrayOf(Schema.of(Schema.Type.STRING));
      }

      if (spannerType.startsWith(SpannerArrayConstants.ARRAY_BYTES_PREFIX)) {
        return Schema.arrayOf(Schema.of(Schema.Type.BYTES));
      }

      switch (spannerType) {
        case SpannerArrayConstants.ARRAY_BOOL:
          return Schema.arrayOf(Schema.of(Schema.Type.BOOLEAN));
        case SpannerArrayConstants.ARRAY_INT64:
          return Schema.arrayOf(Schema.of(Schema.Type.LONG));
        case SpannerArrayConstants.ARRAY_FLOAT64:
          return Schema.arrayOf(Schema.of(Schema.Type.DOUBLE));
        case SpannerArrayConstants.ARRAY_DATE:
          return Schema.arrayOf(Schema.of(Schema.LogicalType.DATE));
        case SpannerArrayConstants.ARRAY_TIMESTAMP:
          return Schema.arrayOf(Schema.of(Schema.LogicalType.TIMESTAMP_MICROS));
        default:
          collector.addFailure(String.format("Column '%s' is of unsupported type 'array'.", columnName),
                               null);
      }
    } else if (spannerType.startsWith("STRING")) {
      // STRING and BYTES also have size at the end in the format, example : STRING(1024)
      return Schema.of(Schema.Type.STRING);
    } else if (spannerType.startsWith("BYTES")) {
      return Schema.of(Schema.Type.BYTES);
    } else {
      switch (Type.Code.valueOf(spannerType)) {
        case BOOL:
          return Schema.of(Schema.Type.BOOLEAN);
        case INT64:
          return Schema.of(Schema.Type.LONG);
        case FLOAT64:
          return Schema.of(Schema.Type.DOUBLE);
        case DATE:
          return Schema.of(Schema.LogicalType.DATE);
        case TIMESTAMP:
          return Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);
        default:
          collector.addFailure(String.format("Column '%s' has unsupported type '%s'.", columnName, spannerType),
                               null);
      }
    }
    return null;
  }
}
