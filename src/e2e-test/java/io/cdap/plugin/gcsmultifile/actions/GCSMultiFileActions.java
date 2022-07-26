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
package io.cdap.plugin.gcsmultifile.actions;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.gcsmultifile.locators.GCSMultiFileLocators;

import java.io.IOException;
import java.util.UUID;

/**
 * GCSMultiFile connector related actions.
 */
public class GCSMultiFileActions {

  static {
    SeleniumHelper.getPropertiesLocators(GCSMultiFileLocators.class);
  }

  public static void enterReferenceName() {
    ElementHelper.sendKeys(GCSMultiFileLocators.referenceName, UUID.randomUUID().toString());
  }

  public static void enterProjectId(String projectId) throws IOException {
    ElementHelper.replaceElementValue(GCSMultiFileLocators.projectId, projectId);
  }

  public static void enterSplitField(String splitField) throws IOException {
    ElementHelper.replaceElementValue(GCSMultiFileLocators.splitField, splitField);
  }

  public static void enterGcsMultiFilepath(String bucket) throws IOException {
    ElementHelper.sendKeys(GCSMultiFileLocators.pathField, bucket);
  }

  public static void selectAllowFlexibleSchema() throws IOException {
    ElementHelper.clickOnElement(GCSMultiFileLocators.allowFlexible);
  }

  public static void selectFormat(String formatType) throws InterruptedException {
    ElementHelper.selectDropdownOption(GCSMultiFileLocators.format,
                                       CdfPluginPropertiesLocators.locateDropdownListItem(formatType));
  }

  public static void closeGcsMultiFile() {
    ElementHelper.clickOnElement(GCSMultiFileLocators.closeButton);
  }

  public static void selectCodec(String codecType) throws InterruptedException {
    ElementHelper.selectDropdownOption(GCSMultiFileLocators.selectCodec,
                                       CdfPluginPropertiesLocators.locateDropdownListItem(codecType));
  }

  public static void selectContentType(String contentType) throws InterruptedException {
    ElementHelper.selectDropdownOption(GCSMultiFileLocators.selectContentType,
                                       CdfPluginPropertiesLocators.locateDropdownListItem(contentType));
  }

  public static void enterDelimiter(String delimiter) {
    ElementHelper.sendKeys(GCSMultiFileLocators.delimiter, delimiter);
  }
}
