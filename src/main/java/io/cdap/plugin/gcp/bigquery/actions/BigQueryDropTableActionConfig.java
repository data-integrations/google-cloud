/*
 * Copyright © 2018 Cask Data, Inc.
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

import com.google.cloud.ServiceOptions;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.etl.api.validation.InvalidConfigPropertyException;
import io.cdap.plugin.gcp.common.GCPConfig;

import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * This class <code>BigQueryDropTableActionConfig</code> provides all the configuration required for
 * configuring the <code>BigQueryDropTableAction</code> plugin.
 */
public class BigQueryDropTableActionConfig extends GCPConfig {
  private static final String BQ_NAME_REGEX = "^[_a-zA-Z0-9]{1,1024}$";

  @Macro
  @Description("The dataset the table belongs to. A dataset is contained within a specific project. "
      + "Datasets are top-level containers that are used to organize and control access to tables and views.")
  private String dataset;

  @Macro
  @Description("The table to be deleted.")
  private String table;

  @Macro
  @Nullable
  @Description("The project the dataset belongs to. This is only required if the dataset is not "
      + "in the same project that the BigQuery job will run in. If no value is given, it will default to the "
      + "configured project ID.")
  private String datasetProject;

  @Description("Drop table only if it exists or attempt to drop unconditionally (may result in pipeline "
      + "failure if table doesn't exist).")
  private Boolean dropOnlyIfExists;

  public String getDataset() {
    return dataset;
  }

  public String getTable() {
    return table;
  }

  public Boolean getDropOnlyIfExists() {
    return dropOnlyIfExists;
  }

  public String getDatasetProject() {
    if (GCPConfig.AUTO_DETECT.equalsIgnoreCase(datasetProject)) {
      return ServiceOptions.getDefaultProjectId();
    }
    return datasetProject == null ? getProject() : datasetProject;
  }

  public void validate() {
    String table = getTable();
    String dataset = getDataset();

    Pattern p = Pattern.compile(BQ_NAME_REGEX);

    if (!p.matcher(table).matches()) {
      throw new InvalidConfigPropertyException("Table name can only contain lowercase characters, " +
          "numbers and underscores.", "table");
    }
    if (!p.matcher(dataset).matches()) {
      throw new InvalidConfigPropertyException("Dataset name can only contain lowercase characters, " +
          "numbers and underscores.", "dataset");
    }
  }
}
