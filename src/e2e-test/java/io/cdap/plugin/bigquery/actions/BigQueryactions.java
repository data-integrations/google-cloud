/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.plugin.bigquery.actions;

import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.bigquery.locators.BigQueryLocators;

/**
 * BigQuery plugin step actions.
 */
public class BigQueryactions {

  static {
    SeleniumHelper.getPropertiesLocators(BigQueryLocators.class);
  }

  public static void enterTableKey(String value) {

    ElementHelper.sendKeys(BigQueryLocators.tableKey, value);
  }

  public static void enterClusterOrder(String value) {
    ElementHelper.sendKeys(BigQueryLocators.clusterOrder, value);

  }

    public static void enterSqlStatement(String value) {
      ElementHelper.sendKeys(BigQueryLocators.sqlStatement, value);

    }
  }