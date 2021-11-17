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
package io.cdap.plugin.file.actions;

import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.file.locators.CdfFileLocators;
import io.cdap.plugin.utils.CdapUtils;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.io.IOException;
import java.util.List;

import static io.cdap.e2e.utils.ConstantsUtil.AUTOMATION_TEST;
import static io.cdap.e2e.utils.ConstantsUtil.DATASET;
import static io.cdap.e2e.utils.ConstantsUtil.PROJECT_ID;

/**
 * File connector related Step Actions.
 */

public class CdfFileActions {

  public static CdfFileLocators cdfFileLocators = null;
  public static CdfBigQueryPropertiesLocators cdfBigQueryPropertiesLocators;
  static {
    cdfFileLocators = SeleniumHelper.getPropertiesLocators(CdfFileLocators.class);
    cdfBigQueryPropertiesLocators = SeleniumHelper.getPropertiesLocators(CdfBigQueryPropertiesLocators.class);
  }

  public static void enterBigQueryPropertiesWithJosn(String demoCheck1) throws IOException {
    CdfStudioLocators.bigQueryProperties.click();
    CdfBigQueryPropertiesLocators.bigQueryReferenceName.sendKeys(AUTOMATION_TEST);
    CdfBigQueryPropertiesLocators.projectID.sendKeys(SeleniumHelper.readParameters(PROJECT_ID));
    SeleniumHelper.replaceElementValue(CdfBigQueryPropertiesLocators.projectID,
                                       SeleniumHelper.readParameters(PROJECT_ID));

    CdfBigQueryPropertiesLocators.bigQueryDataSet.sendKeys(SeleniumHelper.readParameters(DATASET));
    CdfFileLocators.bigQueryJson.click();
    CdfFileLocators.bigQueryServiceAccountJSON.sendKeys
      ("{\n" +
         "  \"type\": \"service_account\",\n" +
         "  \"project_id\": \"cdf-athena\",\n" +
         "  \"private_key_id\": \"183550ea951a8037d57d9b41f660f4581191f3d7\",\n" +
         "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC9fBdq" +
         "iHmZsxxk\\nhcvpCy93gX6iDHAnCanxdgDm3K0rYO2W+elONxAy5r7uCX4LxwqHsxIRSc385XVw" +
         "\\nUqbdQ96yKEhbbWtcZYV4ROY0LcSCRnXwvu3E8F1K8pqli7brEOFD/mySayi99WCN" +
         "\\nOmpwB1EVbdlmaWEwT97TWOaOgCXQnmmu5QnlNnt5hmHiLQEWCIFR6MDZdi3f6PE7" +
         "\\njm29nhNWrddA1B6gpRlAvPw7e11Hd7gTu3DNEkCIixNO874M3jj+oL9odqTdBIa6" +
         "\\nylRhwXZfk9XC4/m/o4+1WO0KFM0z1urcvkJFlZd5kxT8HvoJ/66+Yr7HEKjKxUk7" +
         "\\n68bn4P+BAgMBAAECggEACCzAHYm/8JxcWM7ilk4z+6HT1YhtV0B7ZeOGKZpILfir" +
         "\\nehRpQcVY4XqjLpsCETPYCwoz+O3NoVVB8jkH92r77YTqOD2PK0g6+vkX/b94EfnE" +
         "\\nZqufZPF3c5S7YhyAuqv6rVJe8JiHGbOUzosAYrnCFbIB9iviNEbMhs2NUYUK/Jt3" +
         "\\nTtbJGyaHVrGlPSPXtHn9I5nCNcXtaWx+wuouGFJdUqrD9z7sNEvKIB4jxRi5wZsF" +
         "\\nnn8Pq4ASdr2Me3DudjOT47m4mFSbGs1LckV5brOkUzFLVK+Xr+JMd+oAKgSxkm05" +
         "\\n7N2hCULMZtMmHk9idp7T1CKsxJjuvO4dRR19NGKYxQKBgQDmgWlGAj3hG1T9NSZj" +
         "\\nixEwVHRjw+PJufgo+IYsigNHAmYxMU0c/44oEzlwx9sLbLlOjHO86NOMgWkXbFiP" +
         "\\nhOGdam1ho/rOqmBAKTtLLCi0pR2VCapwQCEYcUOWWYkATZImWxDLaFsfwJSeAMLp" +
         "\\nFrjBZSzs8yKYP9Ex/uZ1DQEfNQKBgQDScTU86MqUFdYSmKlfBG7rYPRS/Gdf4jGA" +
         "\\nx4+VfQ2R2ADB4PBCKIqA4keUD9td0FJFBfRlg6zQKHszUgiSpq1Kz4SuhM6VODoI" +
         "\\nRQr2RSfZM4dHuD5HiGYtZQNljUkKRL6uRSkSEhjmSx/N8ZLCTnq22HWMHbQaOmZI" +
         "\\nqyoQ4TzsnQKBgQC84v8pZ1zdwk/6zjsPBz6mpA5cUoGvJL2+lSkeBlp5LfYgCY5v" +
         "\\nXNtY66f+S0esLQQM4ftVqlTwpns/voEz2mgnXrcTdBRqliMZcLAuAZm5rjR3lNwd" +
         "\\n7+8u4GHKKsShgu9ojudMR8+kTWN7tpQB/aSYlhgic3q92E3M8lxXPrjUoQKBgQCj" +
         "\\n4c+g4HUqP5sXnlWADnbGzRlYKwHiFMeST1bNBrbzlfB5C9Bu5R1/Yzh04Khn9Zyr" +
         "\\n7gg2qgWBkZToEYFlm3GmqdbdBcXPRRtEZ2gzAwYWSt+WBbBSirFvtpOfmRiBa6nH" +
         "\\nE0r5rKhBZ82b1v4AXZeJHybjeBpStpoDC3DV9mI4rQKBgCFHP6L/IflfMeMi7rHM" +
         "\\nm94I8eJZPKMYmZzTtHvt91XAbpUnilz/IMs/2+mu3iP50WM33GvAPbNWgDlPLXbF" +
         "\\ndg6S8fm5n0lUyhKc+2DRHPEevYkH5FNisZuUZsuAGt5oKMZTzPrM7nA1la6BSEOM" +
         "\\nk7T+W4k5tQ+7WQPDAhwM5iTE\\n-----END PRIVATE KEY-----\\n\",\n" +
         "  \"client_email\": \"bq-bqmtb@cdf-athena.iam.gserviceaccount.com\",\n" +
         "  \"client_id\": \"111999633220878599592\",\n" +
         "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
         "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
         "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
         "  \"client_x509_cert_url\": " +
         "\"https://www.googleapis.com/robot/v1/metadata/x509/bq-bqmtb%40cdf-athena.iam.gserviceaccount.com\"\n" +
         "}\n");
    CdfBigQueryPropertiesLocators.bigQueryTable.sendKeys(demoCheck1);
    CdfBigQueryPropertiesLocators.updateTable.click();
    CdfBigQueryPropertiesLocators.truncatableSwitch.click();
    CdfBigQueryPropertiesLocators.validateBttn.click();

  }

  public static void enterFileBucket(String filepath) throws IOException {
    String xyz = CdapUtils.pluginProp(filepath);
    CdfFileLocators.filePath.sendKeys(CdapUtils.pluginProp(filepath));
  }

  static boolean checkparameter = true;

  public static void schemaValidation() {
    List<WebElement> elements = SeleniumDriver.getDriver().findElements(By.xpath("//*[@placeholder='Field name']"));
    for (WebElement schema : elements) {
      String actualStr = schema.getAttribute("value");
      String expectedStr = "offset";
      if (expectedStr.equalsIgnoreCase(actualStr)) {
        checkparameter = false;
      } else {
        checkparameter = true;
      }
    }
  }

  public static void assertionverification() {
    Assert.assertEquals(checkparameter, false);
  }

  public static void selectFile() throws InterruptedException {
    Thread.sleep(3000);
    CdfFileLocators.fileBucket.click();
  }

  public static void enterSamplesize() {
    CdfFileLocators.samplesize.sendKeys("1000");
  }

  public static void actionButton() {
    SeleniumHelper.waitElementIsVisible(CdfFileLocators.actionButton);
    CdfFileLocators.actionButton.click();
  }

  public static void actionDuplicateButton() {
    CdfFileLocators.actionDuplicateButton.click();
  }

  public static void closeButton() {
    CdfFileLocators.closeButton.click();
  }

  public static void fileProperties() {
    CdfFileLocators.fileProperties.click();
  }

  public static void skipHeader() {
    CdfFileLocators.skipHeader.click();
  }

  public static void getSchema() {
    CdfFileLocators.getSchemaButton.click();
  }

  public static void commentWindow() {
    CdfFileLocators.commentWindow.click();
  }

  public static void addcommentWindow() {
    CdfFileLocators.addcommentWindow.sendKeys("Some test demo");
  }

  public static void commentButton() {
    CdfFileLocators.commentButton.click();
  }

  public static void commentWinFileProgerrties() {
    CdfFileLocators.commentWinFileProgerrties.click();
  }

  public static void editComment() {
    CdfFileLocators.editComment.click();
  }

  public static void editCommentButton() {
    CdfFileLocators.editCommentButton.click();
  }

  public static void editDeleteButton() {
    CdfFileLocators.editDeleteButton.click();
  }

}
