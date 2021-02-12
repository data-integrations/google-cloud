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
package io.cdap.plugin.gcp.bigquery.sink;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;

import javax.annotation.Nullable;

/**
 * Provides all the configuration required for configuring the {@link BigQueryMultiSink} plugin.
 */
public class BigQueryMultiSinkConfig extends AbstractBigQuerySinkConfig {

  private static final String SPLIT_FIELD_DEFAULT = "tablename";
  private static final String NAME_ALLOW_FLEXIBLE_SCHEMA = "allowFlexibleSchema";

  @Macro
  @Nullable
  @Description("The name of the field that will be used to determine which table to write to.")
  private String splitField;

  @Name(NAME_ALLOW_FLEXIBLE_SCHEMA)
  @Macro
  @Nullable
  @Description("Allow Flexible Schemas in output. If disabled, only records with schemas set as " +
    "arguments will be processed. If enabled, all records will be written as-is.")
  private Boolean allowFlexibleSchema;

  public String getSplitField() {
    return Strings.isNullOrEmpty(splitField) ? SPLIT_FIELD_DEFAULT : splitField;
  }

  public Boolean getAllowFlexibleSchema() {
    return allowFlexibleSchema != null ? allowFlexibleSchema : false;
  }
}
