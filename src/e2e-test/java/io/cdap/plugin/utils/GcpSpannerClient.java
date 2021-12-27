/*
 * Copyright © 2021 Cask Data, Inc.
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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import io.cucumber.java.en.Then;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Optional;

/**
 * Represents GcpSpannerClient.
 */
public class GcpSpannerClient {

  private static Spanner spannerService = null;
  private static final Logger logger = Logger.getLogger(GcpSpannerClient.class);

  private static Spanner getSpannerService() throws IOException {
    return (null == spannerService) ?
      SpannerOptions.newBuilder().setProjectId(E2ETestUtils.pluginProp("projectId")).build().getService()
      : spannerService;
  }

  public static int countBqQuery(String instanceId, String databaseId, String table)
    throws IOException, InterruptedException {
    String selectQuery = "SELECT count(*) " + " FROM " + table;
    return getSoleQueryResult(instanceId, databaseId, selectQuery).map(Integer::parseInt).orElse(0);
  }

  public static Optional<String> getSoleQueryResult(String instanceId, String databaseId, String query)
    throws InterruptedException, IOException {
    DatabaseClient databaseClient = getSpannerService()
      .getDatabaseClient(DatabaseId.of(getSpannerService().getOptions().getProjectId(), instanceId, databaseId));
    ResultSet resultSet = databaseClient.singleUse().executeQuery(Statement.of(query));
    String abc = null;
    try {
      if (resultSet.next()) {
        abc = String.valueOf(resultSet.getValue(0));
      }
    } catch (SpannerException e) {
      logger.error("Error while fetching data from spanner table : " + e);
    }
    return Optional.ofNullable(abc);
  }
}
