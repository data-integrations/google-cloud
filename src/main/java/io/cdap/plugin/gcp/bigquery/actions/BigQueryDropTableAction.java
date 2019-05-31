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

import com.google.cloud.bigquery.Table;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;

import java.util.Objects;

/**
 * This class <code>BigQueryDropTableAction</code> is a plugin that would allow users
 * to delete BigQuery tables.
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("BigQueryDropTable")
@Description("This action drops given BigQuery table.")
public class BigQueryDropTableAction extends Action {
   private BigQueryDropTableActionConfig config;

   @Override
   public void run(ActionContext actionContext) throws Exception {
      String dataset = config.getDataset();
      String tableNames = config.getTable();
      String project = config.getDatasetProject();
      Table table = BigQueryUtil.getBigQueryTable(config.getServiceAccountFilePath(), project, dataset, tableNames);

      if (Objects.nonNull(table)) {
         table.delete();
      } else if (!config.getDropOnlyIfExists()) {
         // Table does not exist
         throw new IllegalStateException(String.format("BigQuery table '%s:%s.%s' does not exist.",
               project, dataset, tableNames));
      }
   }
}
