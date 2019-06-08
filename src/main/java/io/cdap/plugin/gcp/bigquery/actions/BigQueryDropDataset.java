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

package io.cdap.plugin.gcp.bigquery.actions;

import com.google.auth.Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetId;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;

import javax.ws.rs.NotFoundException;

/**
 * This class <code>BigQueryDropDataset</code> drops a BigQuery dataset.
 * <p>
 *  <code>BigQueryDropDataset</code> can be used to drop BQ dataset in other projects.
 * </p>
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(BigQueryDropDataset.NAME)
@Description(BigQueryDropDataset.DESC)
public class BigQueryDropDataset extends Action {
  public static final String NAME = "BigQueryDropDataset";
  public static final String DESC = "Drops a Cloud BigQuery Dataset";
  private static final String RECORDS_OUT = "records.out";
  private Config config;

  /**
   * Drops a dataset.
   *
   * @param context a <code>ActionContext</code> instance during runtime.
   * @throws Exception thrown when delete of dataset fails.
   */
  @Override
  public void run(ActionContext context) throws Exception {
    String serviceAccountFilePath = config.getServiceAccountFilePath();
    Credentials credentials = serviceAccountFilePath == null ?
      null : GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath);
    BigQuery bq = GCPUtils.getBigQuery(config.getProject(), credentials);
    try {
      Boolean deleted = bq.delete(config.getDatasetId());
      if (deleted) {
        // the dataset was deleted
        context.getMetrics().gauge(RECORDS_OUT, 1);
      } else {
        // the dataset set was not found.
        if (config.failIfNotFound()) {
          throw new NotFoundException(
            String.format("Failed to delete dataset '%s'. Failing pipeline as configured by user.",
                          config.getDatasetName())
          );
        }
      }
    } catch (BigQueryException e) {
      // the dataset delete had an issue.
      if (config.failIfNotFound()) {
        throw new NotFoundException(
          String.format("User configured pipeline to fail if dataset '%s' was not found. Reason : %s",
                        config.getDatasetName(), e.getMessage())
        );
      }
    }
  }

  /**
   * This class <code>Config</code> provides the configuration for deleting a dataset from a project.
   */
  private static final class Config extends GCPConfig {
    @Name("dataset")
    @Description("Dataset to be drop from the project")
    @Macro
    private String dataset;

    @Name("not-found")
    @Description("Choice to fail job if BigQuery dataset is not found.")
    @Macro
    private String failOnNotFound;

    /**
     * @return a instance of <code>DatasetId</code> based on project and dataset name provided by user.
     */
    public DatasetId getDatasetId() {
      return DatasetId.of(getProject(), dataset);
    }

    /**
     * @return a <code>String</code> specifying the name of dataset to be deleted.
     */
    public String getDatasetName() {
      return dataset;
    }

    /**
     * @return a <code>Boolean</code> specifing true if dataset is not found and job should fail, false otherwise.
     */
    public boolean failIfNotFound() {
      if (failOnNotFound.equalsIgnoreCase("true")) {
        return true;
      }
      return false;
    }
  }
}

