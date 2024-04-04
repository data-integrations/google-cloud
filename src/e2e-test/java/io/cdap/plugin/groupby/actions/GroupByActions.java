/*
 * Copyright © 2024 Cask Data, Inc.
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
package io.cdap.plugin.groupby.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.JsonUtils;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.groupby.locators.GroupByLocators;
import io.cucumber.core.logging.Logger;
import io.cucumber.core.logging.LoggerFactory;
import org.openqa.selenium.ElementClickInterceptedException;

import java.util.Map;

/**
 * GroupBy Related Actions.
 */
public class GroupByActions {
  private static final Logger logger = LoggerFactory.getLogger(GroupByActions.class);

  static {
    SeleniumHelper.getPropertiesLocators(GroupByLocators.class);
  }

  /**
   * Enters aggregate fields and their corresponding functions and aliases.
   *
   * @param jsonAggreegatesFields JSON string containing aggregate fields and their functions
   *                              and aliases in key-value pairs
   */
  public static void enterAggregates(String jsonAggreegatesFields) {
    // Convert JSON string to a map of fields and their functions
    Map<String, String> fieldsMapping =
      JsonUtils.convertKeyValueJsonArrayToMap(PluginPropertyUtils.pluginProp(jsonAggreegatesFields));
    int index = 0;
    for (Map.Entry<String, String> entry : fieldsMapping.entrySet()) {
      // Enter field name
      ElementHelper.sendKeys(GroupByLocators.field(index), entry.getKey().split("#")[0]);
      ElementHelper.clickOnElement(GroupByLocators.fieldFunction(index));
      int attempts = 0;
      while (attempts < 5) {
        try {
          ElementHelper.clickOnElement(SeleniumDriver.getDriver().
                                         findElement(CdfPluginPropertiesLocators.locateDropdownListItem
                                           (entry.getKey().split("#")[1])));
          break;
        } catch (ElementClickInterceptedException e) {
          if (attempts == 4) {
            throw e;
          }
        }
        attempts++;
      }
      if (entry.getKey().split("#")[1].contains("If")) {
        ElementHelper.sendKeys(GroupByLocators.fieldFunctionCondition(index), entry.getKey().split("#")[2]);
      }
      ElementHelper.sendKeys(GroupByLocators.fieldFunctionAlias(index), entry.getValue());
      ElementHelper.clickOnElement(GroupByLocators.fieldAddRowButton(index));
      index++;
    }
  }
}
