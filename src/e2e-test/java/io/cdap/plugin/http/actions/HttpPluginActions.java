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

package io.cdap.plugin.http.actions;

import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.http.locators.HttpPluginLocators;
import io.cdap.plugin.utils.CdapUtils;
import io.cdap.plugin.utils.JsonUtils;
import io.cdap.plugin.utils.KeyValue;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.util.List;

import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_COLOR;
import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_HTTP_URL;
import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_HTTP_VALIDATION;
import static io.cdap.plugin.utils.GCConstants.ERROR_MSG_REF;

/**
 * Represents Http plugin related actions.
 */
public class HttpPluginActions {
  static {
      SeleniumHelper.getPropertiesLocators(HttpPluginLocators.class);
      SeleniumHelper.getPropertiesLocators(CdfBigQueryPropertiesLocators.class);
  }

  public static void clickHttpSource() {
    HttpPluginLocators.httpSource.click();
  }

  public static void clickHttpSink() {
    HttpPluginLocators.sink.click();
    HttpPluginLocators.httpSink.click();
  }

  public static void clickHttpSourceHamburgerMenu() {
    SeleniumHelper.waitAndClick(HttpPluginLocators.httpSourceHamburgerMenu);
  }

  public static void clickHttpSourceDeletePlugin() {
    HttpPluginLocators.httpSourceDelete.click();
  }

  public static void clickHttpSinkHamburgerMenu() {
    SeleniumHelper.waitAndClick(HttpPluginLocators.httpSinkHamburgerMenu);
  }

  public static void clickHttpSinkDeletePlugin() {
    HttpPluginLocators.httpSinkDelete.click();
  }

  public static void clickHttpProperties() {
    HttpPluginLocators.httpProperties.click();
  }

  public static void enterReferenceName(String referenceName) {
    HttpPluginLocators.httpReferenceName.sendKeys(referenceName);
  }

  public static void enterHttpUrl(String url) {
    HttpPluginLocators.httpUrl.sendKeys(url);
  }

  public static void selectHttpMethodSource(String httpMethod) {
    HttpPluginLocators.httpMethod.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(
      By.xpath("//li[text()='" + httpMethod + "']")));
  }

  public static void selectHttpMethodSink(String httpMethod) {
    HttpPluginLocators.httpSinkMethod.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(
      By.xpath("//li[text()='" + httpMethod + "']")));
  }

  public static void enterHeadersKey(String key) {
    HttpPluginLocators.headersKey.sendKeys(key);
  }

  public static void enterHeadersValue(String value) {
    HttpPluginLocators.headersValue.sendKeys(value);
  }

  public static void enterRequestBody(String requestBody) {
    HttpPluginLocators.requestBody.sendKeys(requestBody);
  }

  public static void toggleOAuthEnabled() {
    HttpPluginLocators.oAuthEnabled.click();
  }

  public static void enterBasicAuthUserName(String userName) {
    HttpPluginLocators.basicAuthUsername.sendKeys(userName);
  }

  public static void enterBasicAuthPassword(String password) {
    HttpPluginLocators.basicAuthPassword.sendKeys(password);
  }

  public static void selectFormat(String format) {
    HttpPluginLocators.selectFormat.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(
      By.xpath("//li[text()='" + format + "']")));
  }

  public static void selectSinkMessageFormat(String format) {
    HttpPluginLocators.httpSinkMessageFormat.click();
    SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(
      By.xpath("//li[text()='" + format + "']")));
  }

  public static void enterJsonXmlResultPath(String resultPath) {
    HttpPluginLocators.jsonXmlResultPath.sendKeys(resultPath);
  }

  public static void enterJSONXmlFieldsMapping(String jsonFieldsMapping) {
    List<KeyValue> keyValueList = JsonUtils.covertJsonStringToKeyValueList(jsonFieldsMapping);
    int index = 1;
    for (KeyValue keyValue : keyValueList) {
      SeleniumDriver.getDriver().findElement(
        By.xpath("(//*[@data-cy='fieldsMapping']//*[@data-cy='key']/input)[" + index + "]"))
        .sendKeys(keyValue.getKey());
      SeleniumDriver.getDriver().findElement(
        By.xpath("(//*[@data-cy='fieldsMapping']//*[@data-cy='value']/input)[" + index + "]"))
        .sendKeys(keyValue.getValue());
      SeleniumDriver.getDriver().findElement(
        By.xpath("(//*[@data-cy='fieldsMapping']//*[@data-cy='add-row'])[" + index + "]")).click();
      index++;
    }
  }

  public static void enterOutputSchema(String jsonOutputSchema) {
    List<KeyValue> keyValueList = JsonUtils.covertJsonStringToKeyValueList(jsonOutputSchema);
    int index = 0;
    for (KeyValue keyValue : keyValueList) {
      SeleniumDriver.getDriver().findElement(
          By.xpath("//*[@data-cy='schema-fields-list']//*[@data-cy='schema-row-" + index + "']//input"))
        .sendKeys(keyValue.getKey());
      SeleniumDriver.getDriver().findElement(
          By.xpath("//*[@data-cy='schema-fields-list']//*[@data-cy='schema-row-" + index + "']" +
                     "//*[@data-cy='select-undefined']")).click();
      SeleniumHelper.waitAndClick(SeleniumDriver.getDriver().findElement(
        By.xpath("//*[@data-cy='schema-fields-list']//*[@data-cy='schema-row-" + index + "']" +
                   "//*[@data-cy='select-undefined']//*[text()='" + keyValue.getValue() + "']")));
      SeleniumDriver.getDriver().findElement(
          By.xpath("//*[@data-cy='schema-fields-list']//*[@data-cy='schema-row-" + index + "']" +
                     "//input[@type='checkbox']"))
        .click();
      SeleniumDriver.getDriver().findElement(
          By.xpath("//*[@data-cy='schema-fields-list']//*[@data-cy='schema-row-" + index + "']" +
                     "//button[@data-cy='schema-field-add-button']"))
        .click();
      index++;
    }
  }

  public static void clickValidateButton() {
    HttpPluginLocators.validateButton.click();
  }

  public static void clickCloseButton() {
    HttpPluginLocators.closeButton.click();
  }

  public static String validateErrorColor(WebElement element) {
    String color = element.getCssValue(ConstantsUtil.COLOR);
    String[] hexValue = color.replace("rgba(", "").
      replace(")", "").split(",");
    int hexValue1 = Integer.parseInt(hexValue[0]);
    hexValue[1] = hexValue[1].trim();
    int hexValue2 = Integer.parseInt(hexValue[1]);
    hexValue[2] = hexValue[2].trim();
    int hexValue3 = Integer.parseInt(hexValue[2]);
    return String.format("#%02x%02x%02x", hexValue1, hexValue2, hexValue3);
  }

  public static void validateReferenceNameError() {
    String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_REF);
    String actualErrorMessage = HttpPluginLocators.httpReferenceNameError.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = validateErrorColor(HttpPluginLocators.httpReferenceNameError);
    String expectedColor = CdapUtils.errorProp(ERROR_MSG_COLOR);
    Assert.assertEquals(expectedColor, actualColor);
  }

  public static void validateHTTPURLError() {
    String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_HTTP_URL);
    String actualErrorMessage = HttpPluginLocators.httpUrlError.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    String actualColor = validateErrorColor(HttpPluginLocators.httpUrlError);
    String expectedColor = CdapUtils.errorProp(ERROR_MSG_COLOR);
    Assert.assertEquals(expectedColor, actualColor);
  }

  public static void validateValidationError() {
    String expectedErrorMessage = CdapUtils.errorProp(ERROR_MSG_HTTP_VALIDATION);
    String actualErrorMessage = HttpPluginLocators.httpValidationErrorMsg.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
  }

}


