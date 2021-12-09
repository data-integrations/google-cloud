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
import io.cdap.plugin.gcsmultifile.locators.GCSMultifileLocators;
import org.openqa.selenium.By;

import java.io.IOException;
import java.util.UUID;

/**
 * Multifileactions.
 */
public class GCSMulifileActions {

  static {
    SeleniumHelper.getPropertiesLocators(GCSMultifileLocators.class);
    SeleniumHelper.getPropertiesLocators(CdfStudioLocators.class);
  }

  public static void enterReferenceName() {
    GCSMultifileLocators.referenceName.sendKeys(UUID.randomUUID().toString());
  }

  public static void enterProjectId(String projectId) throws IOException {
    SeleniumHelper.replaceElementValue(GCSMultifileLocators.projectId, projectId);
  }

  public static void enterGcsMultifilepath(String bucket) throws IOException {
    SeleniumHelper.replaceElementValue(GCSMultifileLocators.pathField, bucket);
  }

  public static void selectAllowFlexibleSchema() throws IOException {
    GCSMultifileLocators.allowFlexible.click();
  }

  public static void selectFormat(String formatType) throws InterruptedException {
    GCSMultifileLocators.format.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(By.xpath(
      "//*[contains(text(),'" + formatType + "')]")));
  }

  public static void selectGcsMultifile() {
    CdfStudioLocators.sink.click();
    SeleniumHelper.waitAndClick(GCSMultifileLocators.gcsMultiFileObject);
  }

  public static void gcsMultifileProperties() {
    GCSMultifileLocators.gcsMultifileProperties.click();
  }

  public static void closeGcsMultifile() {
    SeleniumHelper.waitElementIsVisible(GCSMultifileLocators.closeButton);
    GCSMultifileLocators.closeButton.click();
  }

  public static void clickSource() {
    GCSMultifileLocators.source.click();
  }

  public static void selectCodec(String codecType) throws InterruptedException {
    GCSMultifileLocators.selectCodec.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(By.xpath(
      "//*[contains(text(),'" + codecType + "')]")));
  }

  public static void selectContentType(String contentType) throws InterruptedException {
    GCSMultifileLocators.selectContentType.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(By.xpath(
      "//*[contains(text(),'" + contentType + "')]")));
  }

  public static void enterDelimiter(String delimiter) {
    GCSMultifileLocators.delimiter.sendKeys(delimiter);
  }
}
