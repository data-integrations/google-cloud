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
package io.cdap.plugin.gcsmove.actions;

import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.gcsmove.locators.GCSMoveLocators;

import java.io.IOException;

/**
 * GCS Move plugin related actions.
 */
public class GCSMoveActions {

  static {
    SeleniumHelper.getPropertiesLocators(GCSMoveLocators.class);
  }

  public static void enterProjectId(String projectId) throws IOException {
    ElementHelper.replaceElementValue(GCSMoveLocators.gcsMoveProjectID, projectId);
  }

  public static void enterSourcePath(String sourcePath) {
    ElementHelper.replaceElementValue(GCSMoveLocators.gcsMoveSourcePath, sourcePath);
  }

  public static void enterDestinationPath(String destinationPath) {
    ElementHelper.replaceElementValue(GCSMoveLocators.gcsMoveDestinationPath, destinationPath);
  }

  public static void enterFileMarkerPath(String path) {
    ElementHelper.replaceElementValue(GCSMoveLocators.gcsDoneFileMarkerPath, path);
  }

  public static void enterServiceAccountFilePath(String path) {
    ElementHelper.replaceElementValue(GCSMoveLocators.gcsServiceAccountFilePath, path);
  }

  public static void selectMoveAllSubdirectories(String value) {
    ElementHelper.selectRadioButton(GCSMoveLocators.moveAllSubdirectories(value));
  }

  public static void selectOverwriteExistingFiles(String value) {
    ElementHelper.selectRadioButton(GCSMoveLocators.overwriteExistingFiles(value));
  }

  public static void enterLocation(String location) {
    ElementHelper.replaceElementValue(GCSMoveLocators.gcsMoveLocation, location);
  }

  public static void enterEncryptionKeyName(String cmek) {
    GCSMoveLocators.gcsMoveEncryptionKey.sendKeys(cmek);
  }
}
