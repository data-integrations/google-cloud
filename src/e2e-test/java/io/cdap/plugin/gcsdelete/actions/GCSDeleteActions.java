/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.plugin.gcsdelete.actions;

import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.gcsdelete.locators.GCSDeleteLocators;

/**
 * GCSDelete plugin step actions.
 */
public class GCSDeleteActions {

  static {
    SeleniumHelper.getPropertiesLocators(GCSDeleteLocators.class);
  }

  public static void enterProjectId(String projectId) {
    ElementHelper.replaceElementValue(GCSDeleteLocators.projectId, projectId);
  }

  public static void enterObjectsToDelete(int row, String objectToDelete) {
    ElementHelper.sendKeys(GCSDeleteLocators.objectsToDeleteInput(row), objectToDelete);
  }

  public static void clickObjectsToDeleteAddRowButton(int row) {
    ElementHelper.clickOnElement(GCSDeleteLocators.objectsToDeleteAddRowButton(row));
  }

  public static void clickObjectsToDeleteRemoveRowButton(int row) {
    ElementHelper.clickOnElement(GCSDeleteLocators.objectsToDeleteRemoveRowButton(row));
  }
}
