/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.plugin.utils;

import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminClient;
import com.google.cloud.bigtable.admin.v2.models.CreateInstanceRequest;
import com.google.cloud.bigtable.admin.v2.models.Instance;
import com.google.cloud.bigtable.admin.v2.models.StorageType;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.commons.nullanalysis.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;


/**
 * Represents GCP CBT Client.
 */
public class BigTableClient {

    private static final Logger logger = LoggerFactory.getLogger(BigTableClient.class);

    public static void createBigTableInstance(BigtableInstanceAdminClient adminClient,
                                              String instanceId, String clusterId) {
        if (!adminClient.exists(instanceId)) {
            CreateInstanceRequest createInstanceRequest = CreateInstanceRequest.of(instanceId)
                            .addCluster(clusterId, "us-central1-f",
                                    3, StorageType.SSD)
                            .setType(Instance.Type.PRODUCTION)
                            .addLabel("it", "e2e-testing");
            try {
                adminClient.createInstance(createInstanceRequest);
            } catch (Exception e) {
                logger.error("Error occuring while creating big table instance", e);
            }
        }
    }

    public static void deleteInstance(BigtableInstanceAdminClient adminClient, String bigTableInstanceId) {
        try {
            adminClient.deleteInstance(bigTableInstanceId);
        } catch (NotFoundException e) {
            logger.error("Error occuring while deleting big table instance", e);
        }
    }

    public static Connection connect(String projectId, String instanceId, @Nullable String serviceAccountCredentials) {
        Configuration configuration = BigtableConfiguration.configure(projectId, instanceId);
        if (serviceAccountCredentials != null) {
            configuration.set(BigtableOptionsFactory.BIGTABLE_SERVICE_ACCOUNT_JSON_VALUE_KEY,
                    serviceAccountCredentials);
        }
        return BigtableConfiguration.connect(configuration);
    }

    public static void createTables(Connection connection,
                                    @Nullable String sourceTable, @Nullable String sinkTable) throws IOException {

        List<String> families = ImmutableList.of("cf1", "cf2");
        if (sourceTable != null) {
            createTable(connection, sourceTable, families);
        }
        if (sinkTable != null) {
            createTable(connection, sinkTable, families);
        }
    }

    public static void createTable(Connection connection,
                                   String dbTableName, List<String> families) throws IOException {
        TableName tableName = TableName.valueOf(dbTableName);
        HTableDescriptor table = new HTableDescriptor(tableName);
        families.stream().map(HColumnDescriptor::new).forEach(table::addFamily);
        if (!connection.getAdmin().tableExists(tableName)) {
            connection.getAdmin().createTable(table);
        }
    }

    public static void populateData(Connection connection, @NotNull String tableName) throws IOException {
        Table sourceTable = getTable(connection, tableName);
        Put put1 = new Put(Bytes.toBytes("r1"));
        put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("boolean_column"), Bytes.toBytes(true));
        put1.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("bytes_column"), Bytes.toBytes("bytes"));
        put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("double_column"), Bytes.toBytes(10.5D));
        put1.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("float_column"), Bytes.toBytes(10.5F));
        put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("int_column"), Bytes.toBytes(1));
        put1.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("long_column"), Bytes.toBytes(10L));
        put1.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("string_column"), Bytes.toBytes("string"));
        sourceTable.put(put1);
    }

    public static Table getTable(Connection connection, String dbTableName) throws IOException {
        TableName tableName = TableName.valueOf(dbTableName);
        return connection.getTable(tableName);
    }
}
