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
package io.cdap.plugin.gcscreate.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * GCSCreate Related Locators.
 */
public class GCSCreateLocators {

  @FindBy(how = How.XPATH, using = "//input[@data-cy='project']")
  public static WebElement projectId;

  public static WebElement objectsToCreateInput(int row) {
    String xpath = "//*[@data-cy='paths']//*[@data-cy= '" + row + "']//*[@data-cy='key']/input";
    return SeleniumDriver.getDriver().findElement(By.xpath(xpath));
  }

  public static WebElement failIfObjectsExistsInput(String value) {
    return SeleniumDriver.getDriver().findElement(By.xpath("//input[@name='failIfExists' and @value='" + value + "']"));
  }
}
