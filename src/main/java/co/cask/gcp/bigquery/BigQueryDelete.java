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
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.DatasetId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * This class <code>BigQueryDelete</code> deletes a dataset or a table.
 * <p>
 * If the table or dataset does not exist, the delete action is still successful.
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
    BigQuery bigQuery = BigQueryUtils.getBigQuery(config.getServiceAccountFilePath(), config.getDatasetProject());

    // if a table is specified it is a delete a table command
    if (config.getTable() != null) {
      if (!bigQuery.delete(config.getDataset(), config.getTable())) {
        LOG.info("Failed to delete table '%s'. Table '%s' does not exist in dataset '%s'.", config.getTable(),
                 config.getTable(), config.getDatasetProject());
      }
    } else {
      boolean deleted;
      if (config.shouldDeleteContents()) {
        deleted = bigQuery.delete(DatasetId.of(config.getDatasetProject(), config.getDataset()),
                                  BigQuery.DatasetDeleteOption.deleteContents());
      } else {
        deleted = bigQuery.delete(DatasetId.of(config.getDatasetProject(), config.getDataset()));
      }
      if (!deleted) {
        LOG.info("Failed to delete dataset '%s'. Dataset '%s' does not exist.", config.getDataset(),
                 config.getDataset());
      }
    }
    context.getMetrics().gauge(RECORDS_OUT, 1);
  }

  /**
   * Config for the plugin.
   */
  public final class Config extends GCPConfig {
    @Description("Name of the dataset to delete.")
    @Macro
    private String dataset;

    @Description("Name of the table to delete. Should be left empty while deleting a dataset.")
    @Macro
    @Nullable
    private String table;

    @Macro
    @Nullable
    @Description("The project the dataset or table belongs to. This is only required if the dataset or table is not "
      + "in the same project that the BigQuery job will run in. If no value is given, it will default to the " +
      "configured project ID.")
    private String datasetProject;

    @Macro
    @Description("Deletes a dataset even if non-empty. Defaults to 'false'")
    private String deleteContents;

    public String getDataset() {
      return dataset;
    }

    public boolean shouldDeleteContents() {
      return deleteContents.equalsIgnoreCase("true");
    }

    @Nullable
    public String getTable() {
      return table;
    }

    public String getDatasetProject() {
      if (GCPConfig.AUTO_DETECT.equalsIgnoreCase(datasetProject)) {
        return ServiceOptions.getDefaultProjectId();
      }
      return datasetProject == null ? getProject() : datasetProject;
    }
  }
}
