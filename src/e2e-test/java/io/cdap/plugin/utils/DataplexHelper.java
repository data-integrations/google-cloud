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
package io.cdap.plugin.utils;


import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.io.IOException;
import java.util.UUID;

/**
 * Dataplex E2E tests helpers.
 */
public class DataplexHelper {
  public static String getCssSelectorByDataTestId (String dataTestId) {
    return "[data-testid=" + dataTestId + "]";
  }

  public static WebElement locateElementByCssSelector(String cssSelector) {
    return SeleniumDriver.getDriver()
      .findElement(By.cssSelector(cssSelector));
  }

  public static WebElement locateElementByXPath(String xpath) {
    return SeleniumDriver.getDriver()
      .findElement(By.xpath(xpath));
  }

  public static void enterReferenceName() {
    ElementHelper.sendKeys(locateElementByXPath(
      "//input[@data-testid='referenceName']"
    ), "Dataplex_Ref_" + UUID.randomUUID());
  }

  public static void enterProjectId() throws IOException {
    ElementHelper.replaceElementValue(locateElementByXPath(
      "//input[@data-testid='project']"
    ), PluginPropertyUtils.pluginProp("projectId"));
  }

  public static void enterDataplexProperty (String prop, String value) {
    ElementHelper.sendKeys(locateElementByXPath(
      "//input[@data-testid='" + prop + "']"
    ), value);
  }

  public static void setAssetType (String assetType) {
    String xpath = "//input[@name='assetType' and @value='" + assetType + "']";
    ElementHelper.selectRadioButton(locateElementByXPath(xpath));
  }

  public static void toggleMetadataUpdate() {
    ElementHelper.clickOnElement(locateElementByCssSelector(
      getCssSelectorByDataTestId("switch-updateDataplexMetadata")
    ));
  }

  public static void deleteSchemaField(String fieldName) {
    String xpath = "//div[contains(@data-cy,'Output Schema')]//input[@value='" + fieldName + "']" +
      "/ancestor::div[contains(@data-cy,'schema-row')]" + "//*[@data-cy='schema-field-remove-button']";
    ElementHelper.clickOnElement(locateElementByXPath(xpath));

  }

}
