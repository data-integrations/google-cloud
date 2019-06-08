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
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

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
  private static final Logger LOG = LoggerFactory.getLogger(BigQueryCreateDataset.class);
  public static final String NAME = "BigQueryCreateDataset";
  public static final String DESC = "Create a Cloud BigQuery Dataset";
  private static final String RECORDS_OUT = "records.out";
  private Config config;

  /**
   * Creates a dataset.
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
      Dataset dataset = bq.getDataset(config.getDatasetId());
      if (dataset == null) {
        dataset = bq.create(config.getDatasetInfo());
        try {
          Dataset.Builder builder = dataset.toBuilder();
          if (config.getDescription() != null) {
            builder.setDescription(config.getDescription());
          }
          if (config.getLabels().size() > 0) {
            builder.setLabels(config.getLabels());
          }
          // update dataset.
          Dataset updateDataset = builder.build();
          updateDataset.update();
        } catch (BigQueryException e) {
          // we don't fail if we cannot update dataset.
          LOG.warn(
            String.format("Dataset '%s' was created, but failed to update metadata for dataset.",
                          config.getDatasetName())
          );
        }
        context.getMetrics().count(RECORDS_OUT, 1);
      } else {
        if (config.failIfExists()) {
          throw new Exception(
            String.format(
              "Dataset '%s' already exists. Failing pipeline as per user request.", config.getDatasetName()
            )
          );
        }
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
    @Macro
    private String dataset;

    @Name("description")
    @Description("Provide a description for dataset")
    @Nullable
    @Macro
    private String description;

    @Name("fail-if-exists")
    @Description("Fails pipeline if dataset already exists or skip silently")
    @Macro
    private String failIfExists;

    @Name("labels")
    @Description("Labels to be associated with dataset")
    @Nullable
    @Macro
    private String labels;

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
     * @return a <code>String</code> specifying the description of a dataset.
     */
    public String getDescription() {
      return description;
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

    /**
     * @return a <code>Map</code> specifing the key value of labels.
     */
    public Map<String, String> getLabels() {
      Map<String, String> labelMap = new HashMap<>();
      if (labels == null || labels.isEmpty()) {
        return labelMap;
      }
      String[] kvPairs = labels.split(",");
      for (String kvPair : kvPairs) {
        String[] kv = kvPair.split(":");
        labelMap.put(kv[0], kv[1]);
      }
      return labelMap;
    }
  }
}

