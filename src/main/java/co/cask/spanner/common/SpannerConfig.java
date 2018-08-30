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

package co.cask.spanner.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Spanner configuration
 */
public class SpannerConfig extends ReferencePluginConfig {
  // todo CDAP-14233 - add support for array, date and timestamp
  private static final Set<Schema.Type> SUPPORTED_TYPES =
    ImmutableSet.of(Schema.Type.BOOLEAN, Schema.Type.STRING, Schema.Type.LONG, Schema.Type.DOUBLE,
                    Schema.Type.BYTES);

  @Description("Google Cloud Project ID, which uniquely identifies your project." +
    "You can find it on the Dashboard in the Cloud Platform Console.")
  @Macro
  @Nullable
  public String project;

  @Description("Cloud Spanner instance id. " +
    "Uniquely identifies Cloud Spanner instance within your Google Cloud Platform project.")
  @Macro
  public String instance;

  @Description("Cloud Spanner database id. Uniquely identifies your database within the Cloud Spanner instance.")
  @Macro
  public String database;

  @Name("serviceFilePath")
  @Description("Path on the local file system to the service account key used for access to Spanner. " +
    "Does not need to be specified when running on a Dataproc cluster. " +
    "When running on other clusters, the file must be present on every node in the cluster.")
  @Macro
  @Nullable
  public String serviceAccountFilePath;

  @Name("schema")
  @Description("Schema of the Spanner table.")
  @Macro
  public String schema;

  public SpannerConfig(String referenceName) {
    super(referenceName);
  }

  public Schema getSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema.");
    }
  }

  public void validate() {
    Schema schema = getSchema();
    for (Schema.Field field : schema.getFields()) {
      Schema fieldSchema = field.getSchema();
      Schema.Type type = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() : fieldSchema.getType();
      if (!SUPPORTED_TYPES.contains(type)) {
        throw new IllegalArgumentException(String.format("Schema type %s not supported by Spanner source", type));
      }
    }
  }
}
