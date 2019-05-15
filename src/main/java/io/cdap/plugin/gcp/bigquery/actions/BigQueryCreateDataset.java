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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.GCPConfig;

/**
 * This class <code>BigQueryCreateDataset</code> drops a BigQuery dataset.
 * <p>
 *  <code>BigQueryCreateDataset</code> can be used to drop BQ dataset in other projects.
 * </p>
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(BigQueryCreateDataset.NAME)
@Description(BigQueryCreateDataset.DESC)
public class BigQueryCreateDataset extends Action {
  public static final String NAME = "BigQueryCreateDataset";
  public static final String DESC = "Create a Cloud BigQuery Dataset";
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
    BigQuery bq = BigQueryUtil.getBigQuery(config.getServiceAccountFilePath(), config.getProject());
    try {
      Dataset dataset = bq.getDataset(config.getDatasetId());
      if (dataset == null) {
        dataset = bq.create(config.getDatasetInfo());
      } else {
        throw new Exception(
          String.format(
            "Dataset '%s' already exists. Failing pipeline as per user configuration.", config.getDatasetName()
          )
        );
      }
    } catch (BigQueryException e) {
      throw new Exception(
        String.format(
          "Failed to create dataset '%s'. Reason : %s", config.getDatasetName(), e.getMessage()
        )
      );
    }
  }

  /**
   * This class <code>Config</code> provides the configuration for deleting a dataset from a project.
   */
  private static final class Config extends GCPConfig {
    @Name("dataset")
    @Description("Dataset to be drop from the project")
    private String dataset;

    @Name("fail-if-exists")
    @Description("Fails pipeline if dataset exists.")
    private String failIfExists;

    /**
     * @return a instance of <code>DatasetId</code> based on project and dataset name provided by user.
     */
    public DatasetId getDatasetId() {
      return DatasetId.of(getProject(), dataset);
    }

    /**
     * @return a instance of <code>DatasetInfo</code> based on project and dataset name provided by user.
     */
    public DatasetInfo getDatasetInfo() {
      return DatasetInfo.newBuilder(dataset).build();
    }

    /**
     * @return a <code>String</code> specifying the name of dataset to be deleted.
     */
    public String getDatasetName() {
      return dataset;
    }

    /**
     * @return a <code>Boolean</code> specifing true if to fail a pipeline if dataset already exists.
     */
    public boolean failIfExists() {
      if (failIfExists.equalsIgnoreCase("true")) {
        return true;
      }
      return false;
    }
  }
}

