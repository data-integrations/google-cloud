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
  package io.cdap.plugin.utils;

import io.cdap.e2e.utils.SeleniumDriver;
import org.apache.log4j.Logger;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * CdapUtils contains the helper functions.
 */
public class CdapUtils {

  private static Properties pluginProperties = new Properties();
  private static Properties errorProperties = new Properties();
  private static final Logger logger = Logger.getLogger(CdapUtils.class);

  static {

    try {
      pluginProperties.load(new FileInputStream("src/e2e-test/resources/pluginParameters.properties"));
      errorProperties.load(new FileInputStream("src/e2e-test/resources/errorMessage.properties"));
    } catch (IOException e) {
      logger.error("Error while reading properties file" + e);
    }
  }

  public static String pluginProp(String property) {
    return pluginProperties.getProperty(property);
  }

  public static String errorProp(String property) {
    return errorProperties.getProperty(property);
  }

  public static boolean schemaValidation() {
    boolean schemaCreated = false;
    List<WebElement> elements = SeleniumDriver.getDriver().findElements(By.xpath("//*[@placeholder='Field name']"));
    if (elements.size() > 2) {
      schemaCreated = true;
      return schemaCreated;
    }
    return false;
  }
}
