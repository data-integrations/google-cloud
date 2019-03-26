/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.gcp.bigquery.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcp.common.GCPConfig;
import co.cask.gcp.common.GCPReferenceSourceConfig;
import com.google.cloud.ServiceOptions;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Holds configuration required for configuring {@link BigQuerySource}.
 */
public final class BigQuerySourceConfig extends GCPReferenceSourceConfig {
  @Macro
  @Description("The dataset the table belongs to. A dataset is contained within a specific project. "
    + "Datasets are top-level containers that are used to organize and control access to tables and views.")
  private String dataset;

  @Macro
  @Description("The table to read from. A table contains individual records organized in rows. "
    + "Each record is composed of columns (also called fields). "
    + "Every table is defined by a schema that describes the column names, data types, and other information.")
  private String table;

  @Macro
  @Nullable
  @Description("The Google Cloud Storage bucket to store temporary data in. "
    + "It will be automatically created if it does not exist, but will not be automatically deleted. "
    + "Temporary data will be deleted after it has been read. "
    + "If it is not provided, a unique bucket will be created and then deleted after the run finishes. "
    + "The service account must have permission to create buckets in the configured project.")
  private String bucket;

  @Macro
  @Description("The schema of the table to read.")
  private String schema;

  @Macro
  @Nullable
  @Description("The project the dataset belongs to. This is only required if the dataset is not "
    + "in the same project that the BigQuery job will run in. If no value is given, it will default to the configured "
    + "project ID.")
  private String datasetProject;

  public String getDataset() {
    return dataset;
  }

  public String getTable() {
    return table;
  }

  @Nullable
  public String getBucket() {
    return bucket;
  }

  public String getDatasetProject() {
    if (GCPConfig.AUTO_DETECT.equalsIgnoreCase(datasetProject)) {
      return ServiceOptions.getDefaultProjectId();
    }
    return datasetProject == null ? getProject() : datasetProject;
  }

  /**
   * @return the schema of the dataset
   * @throws IllegalArgumentException if the schema is null or invalid
   */
  public Schema getSchema() {
    if (schema == null) {
      throw new IllegalArgumentException("Schema must be specified.");
    }
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
    }
  }
}
