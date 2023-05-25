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
package io.cdap.plugin.gcscreate.actions;

import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.gcscreate.locators.GCSCreateLocators;

/**
 * GCSCreate plugin step actions.
 */

public class GCSCreateActions {

  static {
    SeleniumHelper.getPropertiesLocators(GCSCreateLocators.class);
  }

  public static void enterProjectId(String projectId) {
    ElementHelper.replaceElementValue(GCSCreateLocators.projectId, projectId);
  }

  public static void enterObjectsToCreate(int row, String objectToCreate) {
    ElementHelper.sendKeys(GCSCreateLocators.objectsToCreateInput(row), objectToCreate);
  }

  public static void selectFailIfObjectsExists(String value) {
    ElementHelper.selectRadioButton(GCSCreateLocators.failIfObjectsExistsInput(value));
  }
}
