/*
 * Copyright © 2021 Cask Data, Inc.
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

import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.gcsmultifile.locators.GCSMultiFileLocators;
import org.openqa.selenium.By;

import java.io.IOException;
import java.util.UUID;

/**
 * GCSMultiFile connector related actions.
 */
public class GCSMultiFileActions {

  static {
    SeleniumHelper.getPropertiesLocators(GCSMultiFileLocators.class);
    SeleniumHelper.getPropertiesLocators(CdfStudioLocators.class);
  }

  public static void enterReferenceName() {
    GCSMultiFileLocators.referenceName.sendKeys(UUID.randomUUID().toString());
  }

  public static void enterProjectId(String projectId) throws IOException {
    SeleniumHelper.replaceElementValue(GCSMultiFileLocators.projectId, projectId);
  }

  public static void enterGcsMultiFilepath(String bucket) throws IOException {
    SeleniumHelper.replaceElementValue(GCSMultiFileLocators.pathField, bucket);
  }

  public static void selectAllowFlexibleSchema() throws IOException {
    GCSMultiFileLocators.allowFlexible.click();
  }

  public static void selectFormat(String formatType) throws InterruptedException {
    GCSMultiFileLocators.format.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().
                                  findElement(By.xpath("//li[text()='" + formatType + "']")));
  }

  public static void selectGcsMultiFile() {
    CdfStudioLocators.sink.click();
    SeleniumHelper.waitAndClick(GCSMultiFileLocators.gcsMultiFileObject);
  }

  public static void gcsMultiFileProperties() {
    GCSMultiFileLocators.gcsMultiFileProperties.click();
  }

  public static void closeGcsMultiFile() {
    SeleniumHelper.waitElementIsVisible(GCSMultiFileLocators.closeButton);
    GCSMultiFileLocators.closeButton.click();
  }

  public static void clickSource() {
    GCSMultiFileLocators.source.click();
  }

  public static void selectCodec(String codecType) throws InterruptedException {
    GCSMultiFileLocators.selectCodec.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(By.xpath(
      "//*[contains(text(),'" + codecType + "')]")));
  }

  public static void selectContentType(String contentType) throws InterruptedException {
    GCSMultiFileLocators.selectContentType.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(By.xpath(
      "//*[contains(text(),'" + contentType + "')]")));
  }

  public static void enterDelimiter(String delimiter) {
    GCSMultiFileLocators.delimiter.sendKeys(delimiter);
  }
}
