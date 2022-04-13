/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.Database;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Instance;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.InstanceConfigId;
import com.google.cloud.spanner.InstanceId;
import com.google.cloud.spanner.InstanceInfo;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.spanner.admin.database.v1.CreateDatabaseMetadata;
import com.google.spanner.admin.instance.v1.CreateInstanceMetadata;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.PluginPropertyUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

/**
 * Represents GCP Spanner Client.
 */
public class SpannerClient {

  public static int getCountOfRecordsInTable(String instanceId, String databaseId, String table) {
    String selectQuery = "SELECT count(*) " + " FROM " + table;
    return getSoleQueryResult(instanceId, databaseId, selectQuery).map(Integer::parseInt).orElse(0);
  }

  public static Optional<String> getSoleQueryResult(String instanceId, String databaseId, String query) {
    try (Spanner spannerService = SpannerOptions.newBuilder()
      .setProjectId(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID)).build().getService()) {
      DatabaseClient databaseClient = spannerService
        .getDatabaseClient(DatabaseId.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID)
          , instanceId, databaseId));
      try (ResultSet resultSet = databaseClient.singleUse().executeQuery(Statement.of(query))) {
        String outputRowValue = null;
        if (resultSet.next()) {
          outputRowValue = String.valueOf(resultSet.getValue(0));
        }
        return Optional.ofNullable(outputRowValue);
      }
    }
  }

  public static Instance createInstance(String instanceId, String configId)
    throws ExecutionException, InterruptedException {
    Instance instance = null;
    try (Spanner spannerService = SpannerOptions.newBuilder()
      .setProjectId(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID)).build().getService()) {
      InstanceAdminClient instanceAdminClient = spannerService.getInstanceAdminClient();
      int nodeCount = 2;
      InstanceInfo instanceInfo =
        InstanceInfo.newBuilder(InstanceId.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), instanceId))
          .setInstanceConfigId(InstanceConfigId.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), configId))
          .setNodeCount(nodeCount)
          .setDisplayName(instanceId)
          .build();
      OperationFuture<Instance, CreateInstanceMetadata> operation =
        instanceAdminClient.createInstance(instanceInfo);
      instance = operation.get();
      return instance;
    }
  }

  public static Database createDatabase(String instance, String database, List<String> listOfCreateTableQueries)
    throws ExecutionException, InterruptedException {
    try (Spanner spannerService = SpannerOptions.newBuilder()
      .setProjectId(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID)).build().getService()) {
      DatabaseAdminClient dbAdminClient = spannerService.getDatabaseAdminClient();
      OperationFuture<Database, CreateDatabaseMetadata> op =
        dbAdminClient.createDatabase(
          instance,
          database, listOfCreateTableQueries);
      return op.get();
    }
  }

  public static void executeDMLQuery(String instance, String database, String query) {
    try (Spanner spannerService = SpannerOptions.newBuilder()
      .setProjectId(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID)).build().getService()) {
      spannerService
        .getDatabaseClient(DatabaseId.of(InstanceId.of(
          PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), instance), database))
        .readWriteTransaction()
        .run(transaction -> transaction.executeUpdate(Statement.of(query)));
    }
  }

  public static void deleteInstance(String instance) {
    try (Spanner spannerService = SpannerOptions.newBuilder()
      .setProjectId(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID)).build().getService()) {
      spannerService.getInstanceAdminClient().getInstance(instance).delete();
    }
  }

  public static String databaseCmekKey(String instance, String database) {
    try (Spanner spannerService = SpannerOptions.newBuilder()
      .setProjectId(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID)).build().getService()) {
      return spannerService.getDatabaseAdminClient()
        .getDatabase(instance, database).getEncryptionConfig().getKmsKeyName();
    }
  }
}
