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
package co.cask.gcp.datastore.source.util;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcp.datastore.exception.DatastoreExecutionException;
import co.cask.gcp.datastore.source.DatastoreSourceConfig;
import com.google.cloud.Timestamp;
import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.DoubleValue;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import com.google.datastore.v1.CompositeFilter;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.KindExpression;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.PropertyFilter;
import com.google.datastore.v1.client.DatastoreHelper;
import com.google.protobuf.TextFormat;
import org.apache.hadoop.conf.Configuration;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Utility class that provides methods to construct and transform queries, filters and keys.
 */
public class DatastoreSourceQueryUtil {

  private static final String NULL_VALUE = "null";

  /**
   * Constructs partition ID instance using project ID and namespace.
   *
   * @param project project ID
   * @param namespace namespace
   * @return return partition ID instance
   */
  public static PartitionId getPartitionId(String project, String namespace) {
    return PartitionId.newBuilder()
      .setProjectId(project)
      .setNamespaceId(namespace)
      .build();
  }

  /**
   * Constructs ancestor key using using given Datastore configuration.
   *
   * @param config Datastore configuration
   * @return Datastore key instance
   */
  public static Key constructAncestorKey(DatastoreSourceConfig config) {
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
   * Transforms Datastore key to its string representation based on given key type.
   *
   * @param key Datastore key instance
   * @param keyType key type
   * @return key string representation
   */
  public static String transformKeyToKeyString(Key key, SourceKeyType keyType) {
    switch (keyType) {
      case KEY_LITERAL:
        StringBuilder builder = new StringBuilder("key(");

        key.getAncestors()
          .forEach(a -> appendKindWithNameOrId(builder, a.getKind(), a.getNameOrId()).append(", "));

        appendKindWithNameOrId(builder, key.getKind(), key.getNameOrId());
        return builder.append(")").toString();

      case URL_SAFE_KEY:
        return key.toUrlSafe();

      default:
        throw new IllegalArgumentException(
          String.format("Unable to transform key [%s] to type [%s] string representation", key, keyType.getValue()));
    }
  }

  /**
   * Constructs protobuf query instance which will be used for query splitting.
   * Adds ancestor and property filters if present in the given configuration.
   *
   * @param config Datastore configuration
   * @return protobuf query instance
   */
  public static com.google.datastore.v1.Query constructPbQuery(DatastoreSourceConfig config) {
    com.google.datastore.v1.Query.Builder builder = com.google.datastore.v1.Query.newBuilder()
      .addKind(KindExpression.newBuilder()
                 .setName(config.getKind()));

    List<Filter> filters = config.getFilters().entrySet().stream()
      .map(e -> DatastoreHelper.makeFilter(e.getKey(), PropertyFilter.Operator.EQUAL,
            constructFilterValue(e.getKey(), e.getValue(), config.getSchema()))
        .build())
      .collect(Collectors.toList());

    List<PathElement> ancestors = config.getAncestor();
    if (!ancestors.isEmpty()) {
      filters.add(DatastoreHelper.makeAncestorFilter(constructKey(ancestors, config)).build());
    }

    if (!filters.isEmpty()) {
      builder.setFilter(DatastoreHelper.makeAndFilter(filters));
    }

    return builder.build();
  }

  /**
   * Transforms protobuf query instance emitted by the query splitter into {@link Query<Entity>}.
   *
   * @param pbQuery protobuf query instance
   * @param config Hadoop configuration
   * @return query instance
   */
  public static Query<Entity> transformPbQuery(com.google.datastore.v1.Query pbQuery, Configuration config) {
    EntityQuery.Builder builder = Query.newEntityQueryBuilder()
      .setNamespace(config.get(DatastoreSourceConstants.CONFIG_NAMESPACE))
      .setKind(config.get(DatastoreSourceConstants.CONFIG_KIND));

    if (pbQuery.hasFilter()) {
      builder.setFilter(transformFilterFromPb(pbQuery.getFilter()));
    }

    return builder.build();
  }

  /**
   * Constructs Datastore protobuf key instance based on given list of path elements
   * and Datastore configuration.
   *
   * @param pathElements list of path elements
   * @param config Datastore configuration
   * @return Datastore protobuf key instance
   */
  private static com.google.datastore.v1.Key constructKey(List<PathElement> pathElements,
                                                          DatastoreSourceConfig config) {
    Object[] elements = pathElements.stream()
      .flatMap(pathElement -> Stream.of(pathElement.getKind(), pathElement.getNameOrId()))
      .toArray();

    return DatastoreHelper.makeKey(elements)
      .setPartitionId(getPartitionId(config.getProject(), config.getNamespace()))
      .build();
  }

  /**
   * Appends to the string builder name or id based on given instance type.
   * Name will be enclosed into the single quotes.
   *
   * @param builder string builder
   * @param kind kind name
   * @param nameOrId object representing name or id
   * @return updated string builder
   */
  private static StringBuilder appendKindWithNameOrId(StringBuilder builder, String kind, Object nameOrId) {
    builder.append(kind).append(", ");
    if (nameOrId instanceof Long) {
      builder.append(nameOrId);
    } else {
      builder.append("'").append(nameOrId).append("'");
    }
    return builder;
  }

  /**
   * Transforms given value into value holder corresponding to the given field schema type.
   * If given value is {@link DatastoreSourceQueryUtil#NULL_VALUE}, creates null value holder.
   *
   * @param name field name
   * @param value filter value in string representation
   * @param schema field schema
   * @return protobuf value for filter
   */
  private static com.google.datastore.v1.Value constructFilterValue(String name, String value, Schema schema) {
    Schema.Field field = Objects.requireNonNull(schema.getField(name));
    Schema fieldSchema = field.getSchema();

    if (NULL_VALUE.equalsIgnoreCase(value)) {
      return com.google.datastore.v1.Value.newBuilder()
        .setNullValue(com.google.protobuf.NullValue.NULL_VALUE)
        .build();
    }

    return constructFilterValue(fieldSchema, value);
  }

  /**
   * Transforms given value into value holder corresponding to the given field schema type.
   * May call itself recursively of schema is of UNION type.
   *
   * @param schema field schema
   * @param value value in string representation
   * @return protobuf value for filter
   */
  private static com.google.datastore.v1.Value constructFilterValue(Schema schema, String value) {
    switch (schema.getType()) {
      case STRING:
        return DatastoreHelper.makeValue(value).build();
      case DOUBLE:
        return DatastoreHelper.makeValue(Double.valueOf(value)).build();
      case LONG:
        Schema.LogicalType logicalType = schema.getLogicalType();
        if (logicalType == null) {
          return DatastoreHelper.makeValue(Long.valueOf(value)).build();
        }
        if (Schema.LogicalType.TIMESTAMP_MICROS == logicalType) {
          Timestamp timestamp = Timestamp.parseTimestamp(value);
          return com.google.datastore.v1.Value.newBuilder()
            .setTimestampValue(timestamp.toProto())
            .build();
        }
        throw new IllegalArgumentException("Unsupported logical type for Long: " + logicalType);
      case BOOLEAN:
        return DatastoreHelper.makeValue(Boolean.valueOf(value)).build();
      case UNION:
        // nullable fields in CDAP are represented as UNION of NULL and FIELD_TYPE
        if (schema.isNullable()) {
          return constructFilterValue(schema.getNonNullable(), value);
        }
        throw new IllegalArgumentException("Complex UNION type is not supported");
      default:
        throw new IllegalArgumentException("Unsupported filter type: " + schema.getType());
    }
  }

  /**
   * Transforms protobuf filter into {@link StructuredQuery.Filter} instance.
   *
   * @param pbFilter protobuf filter
   * @return query filter
   */
  private static StructuredQuery.Filter transformFilterFromPb(Filter pbFilter) {
    Filter.FilterTypeCase filterType = pbFilter.getFilterTypeCase();
    switch (filterType) {
      case PROPERTY_FILTER:
        return transformPropertyFilterFromPb(pbFilter.getPropertyFilter());
      case COMPOSITE_FILTER:
        CompositeFilter compositeFilter = pbFilter.getCompositeFilter();
        List<StructuredQuery.Filter> filters = new ArrayList<>();
        for (Filter filter : compositeFilter.getFiltersList()) {
          StructuredQuery.Filter transformFilterFromPb = transformFilterFromPb(filter);
          filters.add(transformFilterFromPb);
        }
        if (filters.size() == 1) {
          return StructuredQuery.CompositeFilter.and(filters.get(0));
        }
        StructuredQuery.Filter[] filterArray = filters.subList(1, filters.size())
          .toArray(new StructuredQuery.Filter[filters.size() - 1]);
        return StructuredQuery.CompositeFilter.and(filters.get(0), filterArray);
      default:
        throw new IllegalArgumentException("Unexpected filter type: " + filterType);
    }
  }

  /**
   * Transforms protobuf property filter into {@link StructuredQuery.Filter} instance.
   *
   * @param propertyFilter protobuf property filter
   * @return query property filter
   */
  private static StructuredQuery.Filter transformPropertyFilterFromPb(PropertyFilter propertyFilter) {
    com.google.datastore.v1.Value value = propertyFilter.getValue();
    com.google.datastore.v1.Value.ValueTypeCase valueType = value.getValueTypeCase();
    Value<?> valueHolder;
    switch (valueType) {
      case STRING_VALUE:
        valueHolder = StringValue.of(value.getStringValue());
        break;
      case INTEGER_VALUE:
        valueHolder = LongValue.of(value.getIntegerValue());
        break;
      case DOUBLE_VALUE:
        valueHolder = DoubleValue.of(value.getDoubleValue());
        break;
      case BOOLEAN_VALUE:
        valueHolder = BooleanValue.of(value.getBooleanValue());
        break;
      case TIMESTAMP_VALUE:
        valueHolder = TimestampValue.of(Timestamp.fromProto(value.getTimestampValue()));
        break;
      case KEY_VALUE:
        try {
          String encodedKey = URLEncoder.encode(TextFormat.printToString(value.getKeyValue()), UTF_8.name());
          valueHolder = KeyValue.of(Key.fromUrlSafe(encodedKey));
        } catch (UnsupportedEncodingException e) {
          throw new DatastoreExecutionException("Unable to encode protobuf key value: " + value.getKeyValue());
        }
        break;
      case NULL_VALUE:
        valueHolder = NullValue.of();
        break;
      default:
        throw new IllegalArgumentException("Unexpected value type: " + valueType);
    }

    String name = propertyFilter.getProperty().getName();
    PropertyFilter.Operator operator = propertyFilter.getOp();
    switch (operator) {
      case EQUAL:
        return StructuredQuery.PropertyFilter.eq(name, valueHolder);
      case LESS_THAN:
        return StructuredQuery.PropertyFilter.lt(name, valueHolder);
      case GREATER_THAN:
        return StructuredQuery.PropertyFilter.gt(name, valueHolder);
      case LESS_THAN_OR_EQUAL:
        return StructuredQuery.PropertyFilter.le(name, valueHolder);
      case GREATER_THAN_OR_EQUAL:
        return StructuredQuery.PropertyFilter.ge(name, valueHolder);
      case HAS_ANCESTOR:
        return StructuredQuery.PropertyFilter.hasAncestor((Key) valueHolder.get());
      default:
        throw new IllegalArgumentException("Unexpected operator type: " + operator);
    }
  }

}
