/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.gcp.bigquery.sink;

import com.google.cloud.bigquery.TimePartitioning;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Tests for {@link BigQuerySinkConfig}.
 */

public class BigQuerySinkConfigTest {
    MockFailureCollector collector;
    BigQuerySinkConfig config;
    Method validateTimePartitioningColumnMethod;

    @Before
    public void setup() throws NoSuchMethodException {
        collector = new MockFailureCollector();
        config = BigQuerySinkConfig.builder().build();
        validateTimePartitioningColumnMethod = config.getClass()
                .getDeclaredMethod("validateTimePartitioningColumn", String.class, FailureCollector.class,
                        Schema.class, TimePartitioning.Type.class);
        validateTimePartitioningColumnMethod.setAccessible(true);
    }
    @Test
    public void testValidateTimePartitioningColumnWithHourAndDate() throws
            InvocationTargetException, IllegalAccessException {
        String columnName = "partitionFrom";
        Schema fieldSchema = Schema.recordOf("test", Schema.Field.of("partitionFrom",
                Schema.of(Schema.LogicalType.DATE)));
        TimePartitioning.Type timePartitioningType = TimePartitioning.Type.HOUR;

        validateTimePartitioningColumnMethod.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
        Assert.assertEquals(String.format("Partition column '%s' is of invalid type '%s'.",
                        columnName, fieldSchema.getDisplayName()),
                collector.getValidationFailures().stream().findFirst().get().getMessage());
    }

    @Test
    public void testValidateTimePartitioningColumnWithHourAndTimestamp() throws
            InvocationTargetException, IllegalAccessException {

        String columnName = "partitionFrom";
        Schema schema = Schema.of(Schema.LogicalType.TIMESTAMP_MICROS);

        Schema fieldSchema = schema.isNullable() ? schema.getNonNullable() : schema;
        TimePartitioning.Type timePartitioningType = TimePartitioning.Type.HOUR;

        validateTimePartitioningColumnMethod.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
        Assert.assertEquals(0, collector.getValidationFailures().size());
    }

    @Test
    public void testValidateTimePartitioningColumnWithDayAndString() throws
            InvocationTargetException, IllegalAccessException {

        String columnName = "partitionFrom";
        Schema schema = Schema.of(Schema.Type.STRING);

        Schema fieldSchema = schema.isNullable() ? schema.getNonNullable() : schema;
        TimePartitioning.Type timePartitioningType = TimePartitioning.Type.DAY;

        validateTimePartitioningColumnMethod.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
        Assert.assertEquals(String.format("Partition column '%s' is of invalid type '%s'.",
                        columnName, fieldSchema.getDisplayName()),
                collector.getValidationFailures().stream().findFirst().get().getMessage());
    }

    @Test
    public void testValidateTimePartitioningColumnWithDayAndDate() throws
            InvocationTargetException, IllegalAccessException {

        String columnName = "partitionFrom";
        Schema schema = Schema.of(Schema.LogicalType.DATE);

        Schema fieldSchema = schema.isNullable() ? schema.getNonNullable() : schema;
        TimePartitioning.Type timePartitioningType = TimePartitioning.Type.DAY;

        validateTimePartitioningColumnMethod.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
        Assert.assertEquals(0, collector.getValidationFailures().size());
    }

    @Test
    public void testValidateTimePartitioningColumnWithNullAndDate() throws
            InvocationTargetException, IllegalAccessException {

        String columnName = "partitionFrom";
        Schema schema = Schema.of(Schema.LogicalType.DATE);

        Schema fieldSchema = schema.isNullable() ? schema.getNonNullable() : schema;
        TimePartitioning.Type timePartitioningType = null;

        validateTimePartitioningColumnMethod.invoke(config, columnName, collector, fieldSchema, timePartitioningType);
        // No error as null time timePartitioningType will default to DAY
        Assert.assertEquals(0, collector.getValidationFailures().size());
    }

}
