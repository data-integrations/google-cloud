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

package io.cdap.plugin.datastore.actions;

import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.datastore.locators.DataStoreLocators;
import io.cdap.plugin.utils.DataStoreClient;

/**
 * DataStore Plugin related actions.
 */
public class DataStoreActions {
  static {
    SeleniumHelper.getPropertiesLocators(DataStoreLocators.class);
  }

  /**
   * Enters the specified kind name into the appropriate field in the user interface.
   *
   * @param kindName the name of the kind to be entered
   */
  public static void enterKind(String kindName) {
    ElementHelper.sendKeys(DataStoreLocators.kind, kindName);
  }

  /**
   * Enters the key literal of the current entity into the appropriate field in the user interface
   * as the ancestor.
   */
  public static void enterAncestor() {
    ElementHelper.sendKeys(DataStoreLocators.ancestor, DataStoreClient.getKeyLiteral());
  }
}
