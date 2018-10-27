/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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

package co.cask.gcp.bigquery;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.etl.api.action.Action;
import co.cask.cdap.etl.api.action.ActionContext;
import co.cask.gcp.common.GCPConfig;
import co.cask.gcp.common.GCPUtils;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.DatasetId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * This class <code>BigQueryDelete</code> deletes a dataset or a table.
 *
 * If the table or dataset does not exist, it's still successful.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(BigQueryDelete.NAME)
@Description("Delete a Google BigQuery dataset or table.")
public final class BigQueryDelete extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryDelete.class);
  public static final String NAME = "BigQueryDelete";
  private static final String TABLE = "table";
  private static final String DATASET = "dataset";
  private static final String RECORDS_OUT = "records.out";
  private Config config;

  @Override
  public void run(ActionContext context) throws Exception {
    // API request - starts the query.
    BigQueryOptions.Builder bqBuilder = GCPUtils.getBigQuery(config.getProject(), config.getServiceAccountFilePath());
    BigQuery bigquery = bqBuilder.build().getService();

    // Delete a table.
    if (config.type.equalsIgnoreCase(TABLE)) {
      boolean delete = bigquery.delete(config.dataset, config.table);
      if (!delete) {
        LOG.info("Table %s was deleted", config.table);
      }
    }

    // Delete a dataset
    if (config.type.equalsIgnoreCase(DATASET)) {
      boolean delete = false;
      if (config.getProject() == null) {
        delete = bigquery.delete(DatasetId.of(config.dataset));
      } else {
        delete = bigquery.delete(DatasetId.of(config.getProject(), config.dataset));
      }
      if (!delete) {
        LOG.info("Dataset %s was deleted", config.dataset);
      }
    }

    context.getMetrics().gauge("records.out", 1);
  }

  /**
   * Config for the plugin.
   */
  public final class Config extends GCPConfig {
    @Name("type")
    @Description("Delete table or dataset")
    @Macro
    public String type;

    @Name("dataset")
    @Description("Name of dataset.")
    @Macro
    public String dataset;

    @Name("table")
    @Description("Name of table.")
    @Macro
    @Nullable
    public String table;
  }
}
