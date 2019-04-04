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
package io.cdap.plugin.gcp.datastore.source;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.BooleanValue;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DoubleValue;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyValue;
import com.google.cloud.datastore.LongValue;
import com.google.cloud.datastore.NullValue;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.StructuredQuery;
import com.google.cloud.datastore.TimestampValue;
import com.google.cloud.datastore.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.datastore.v1.CompositeFilter;
import com.google.datastore.v1.Filter;
import com.google.datastore.v1.PropertyFilter;
import com.google.protobuf.TextFormat;
import io.cdap.plugin.gcp.datastore.exception.DatastoreExecutionException;
import io.cdap.plugin.gcp.datastore.source.util.DatastoreSourceConstants;
import io.cdap.plugin.gcp.datastore.util.DatastoreUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Datastore read reader instantiates a record reader that will read the entities from Datastore,
 * using given {@link Query} instance from input split.
 */
public class DatastoreRecordReader extends RecordReader<LongWritable, Entity> {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreRecordReader.class);

  private QueryResults<Entity> results;
  private Entity entity;
  private long index;
  private LongWritable key;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
    Configuration config = taskAttemptContext.getConfiguration();
    Query<Entity> query = transformPbQuery(((QueryInputSplit) inputSplit).getQuery(), config);
    Datastore datastore = DatastoreUtil.getDatastore(
      config.get(DatastoreSourceConstants.CONFIG_SERVICE_ACCOUNT_FILE_PATH),
      config.get(DatastoreSourceConstants.CONFIG_PROJECT));
    LOG.trace("Executing query split: {}", query);
    results = datastore.run(query);
    index = 0;
  }

  @Override
  public boolean nextKeyValue() {
    if (!results.hasNext()) {
      return false;
    }
    entity = results.next();
    key = new LongWritable(index);
    ++index;
    return true;
  }

  @Override
  public LongWritable getCurrentKey() {
    return key;
  }

  @Override
  public Entity getCurrentValue() {
    return entity;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public void close() {
  }

  /**
   * Transforms protobuf query instance emitted by the query splitter into {@link Query<Entity>}.
   *
   * @param pbQuery protobuf query instance
   * @param config  Hadoop configuration
   * @return query instance
   */
  @VisibleForTesting
  Query<Entity> transformPbQuery(com.google.datastore.v1.Query pbQuery, Configuration config) {
    EntityQuery.Builder builder = Query.newEntityQueryBuilder()
      .setNamespace(config.get(DatastoreSourceConstants.CONFIG_NAMESPACE))
      .setKind(config.get(DatastoreSourceConstants.CONFIG_KIND));

    if (pbQuery.hasFilter()) {
      builder.setFilter(transformFilterFromPb(pbQuery.getFilter()));
    }

    return builder.build();
  }

  /**
   * Transforms protobuf filter into {@link StructuredQuery.Filter} instance.
   *
   * @param pbFilter protobuf filter
   * @return query filter
   */
  private StructuredQuery.Filter transformFilterFromPb(Filter pbFilter) {
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
        throw new IllegalStateException(
          String.format("Protobuf filter '%s' has unexpected filter type '%s'", pbFilter, filterType));
    }
  }

  /**
   * Transforms protobuf property filter into {@link StructuredQuery.Filter} instance.
   *
   * @param propertyFilter protobuf property filter
   * @return query property filter
   */
  private StructuredQuery.Filter transformPropertyFilterFromPb(PropertyFilter propertyFilter) {
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
          throw new DatastoreExecutionException(
            String.format("Unable to encode protobuf key value '%s' for property filter '%s'",
                          value.getKeyValue(), propertyFilter));
        }
        break;
      case NULL_VALUE:
        valueHolder = NullValue.of();
        break;
      default:
        throw new IllegalStateException(
          String.format("Property filter '%s' has unexpected value type '%s'", propertyFilter, valueType));
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
        throw new IllegalStateException(
          String.format("Property filter '%s' has unexpected operator type '%s'", propertyFilter, operator));
    }
  }

}
