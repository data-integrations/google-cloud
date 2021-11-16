package io.cdap.plugin.odp.stepsdesign;

import io.cdap.e2e.pages.actions.CdfSysAdminActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.pages.locators.CdfSysAdminLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.odp.actions.ODPActions;
import io.cdap.plugin.odp.locators.ODPLocators;
import io.cdap.plugin.odp.utils.CDAPUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import org.openqa.selenium.Keys;
import org.openqa.selenium.NoAlertPresentException;

import java.io.IOException;
import java.util.HashMap;

/**
 * Security.
 */
public class Security implements CdfHelper {

  String arr[] = new String[11];
  HashMap map = new HashMap();
  public static int countarr = 0;

  @Given("Open {string} link to configure macros")
  public void openLinkToConfigureMacros(String link) throws IOException {
    SeleniumDriver.getDriver().get(CDAPUtils.pluginProp(link));
    SeleniumDriver.waitForPageToLoad();
    try {
      SeleniumDriver.getDriver().switchTo().alert().accept();
      SeleniumDriver.waitForPageToLoad();
    } catch (NoAlertPresentException var3) {
      SeleniumDriver.waitForPageToLoad();
    }
  }


  @Then("Select {string} service to configure")
  public void selectServiceToConfigure(String service) {
    CdfSysAdminActions.selectMacroAPIService(service);
  }

  @Then("enter variable for {string} of the macro")
  public void enterVariableForOfTheMacro(String arg0) {
    countarr = arg0.length();
    for (int i = 0; i < 100; i++) {
      CdfSysAdminLocators.apiInputURI.sendKeys(Keys.BACK_SPACE);
    }
    CdfSysAdminActions.enterURI("namespaces/default/securekeys/" + arg0);
  }

  @Then("enter the {string} of the service")
  public void enterTheOfTheService(String request) throws IOException {
    try {
      for (int i = 0; i <= 100; i++) {
        CdfSysAdminLocators.requestBody.sendKeys(Keys.BACK_SPACE);
      }
    } finally {
      for (int i = 0; i <= 100; i++) {
        CdfSysAdminLocators.requestBody.sendKeys(Keys.BACK_SPACE);
      }
    }
    CdfSysAdminActions.enterRequestBody(CDAPUtils.pluginProp(request));
  }

  @Then("send request and verify success message")
  public void sendRequestAndVerifySuccessMessage() throws IOException {
    CdfSysAdminActions.clearRequest();
    CdfSysAdminActions.clearAllRequest();
    CdfSysAdminActions.sendRequest();
    CdfSysAdminActions.verifySuccess();
  }

  @Then("Link Source and Sink table")
  public void linkSourceAndSinkTable() throws InterruptedException {
    waitForSinkOnCanvas("BigQueryTable");
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromSAPODP, CdfStudioLocators.toBigQiery);
  }

  @Then("enter the macro variable in fields")
  public void enterTheMacroVariableInFields() throws InterruptedException {
    ODPActions.clickAllMacroElements();
    for (int i = 0; i < 10; i++) {
      ODPLocators.usernameCredentials.sendKeys(Keys.BACK_SPACE);
      ODPLocators.macroPass.sendKeys(Keys.BACK_SPACE);
      ODPLocators.sapClient.sendKeys(Keys.BACK_SPACE);
      ODPLocators.systemNumber.sendKeys(Keys.BACK_SPACE);
      ODPLocators.sapApplicationServerHost.sendKeys(Keys.BACK_SPACE);
      ODPLocators.dataSourceName.sendKeys(Keys.BACK_SPACE);
      ODPLocators.packageSize.sendKeys(Keys.BACK_SPACE);
      ODPLocators.splitRow.sendKeys(Keys.BACK_SPACE);
      ODPLocators.gcsPath.sendKeys(Keys.BACK_SPACE);
      ODPLocators.projectID.sendKeys(Keys.BACK_SPACE);
      ODPLocators.language.sendKeys(Keys.BACK_SPACE);
      ODPLocators.extractType.sendKeys(Keys.BACK_SPACE);
    }
    ODPActions.clickMacroElement(4);
    ODPActions.clickMacroElement(10);
    ODPActions.clickMacroElement(11);
    ODPActions.clickMacroElement(12);
    ODPLocators.sapClient.sendKeys("${clientmacro}");
    ODPLocators.language.sendKeys("${languagemacro}");
    ODPLocators.sapApplicationServerHost.sendKeys("${serverhostmacro}");
    ODPLocators.systemNumber.sendKeys("${sysnrmacro}");
    ODPLocators.dataSourceName.sendKeys("${datasourcemacro}");
    ODPLocators.extractType.sendKeys("${loadtype}");
    ODPLocators.usernameCredentials.sendKeys("${usermacro}");
    ODPLocators.macroPass.sendKeys("${passmacro}");
    ODPLocators.projectID.sendKeys("${gcpprojectid}");
    ODPLocators.gcsPath.sendKeys("${gcsbucket}");
    ODPLocators.splitRow.sendKeys("${numbersplit}");
    ODPLocators.packageSize.sendKeys("${packagesize}");
  }

  @Then("enter the secured created variable")
  public void enterTheSecuredCreatedVariable() throws InterruptedException {
    ODPActions.clickAllMacroElements();
    for (int i = 0; i < 4; i++) {
      ODPLocators.sapClient.sendKeys(Keys.BACK_SPACE);
      ODPLocators.language.sendKeys(Keys.BACK_SPACE);
      ODPLocators.sapApplicationServerHost.sendKeys(Keys.BACK_SPACE);
      ODPLocators.systemNumber.sendKeys(Keys.BACK_SPACE);
      ODPLocators.sapRouter.sendKeys(Keys.BACK_SPACE);
      ODPLocators.dataSourceName.sendKeys(Keys.BACK_SPACE);
      ODPLocators.extractType.sendKeys(Keys.BACK_SPACE);
      ODPLocators.usernameCredentials.sendKeys(Keys.BACK_SPACE);
      ODPLocators.macroPass.sendKeys(Keys.BACK_SPACE);
      ODPLocators.gcsProjectID.sendKeys(Keys.BACK_SPACE);
      ODPLocators.packageSize.sendKeys(Keys.BACK_SPACE);
      ODPLocators.splitRow.sendKeys(Keys.BACK_SPACE);
      ODPLocators.gcsPath.sendKeys(Keys.BACK_SPACE);
      ODPLocators.subsName.sendKeys(Keys.BACK_SPACE);
      ODPLocators.filterEqualVal.sendKeys(Keys.BACK_SPACE);
      ODPLocators.filterRangeVal.sendKeys(Keys.BACK_SPACE);
    }
    ODPActions.clickMacroElement(4);
    ODPActions.clickMacroElement(10);
    ODPActions.clickMacroElement(11);
    ODPActions.clickMacroElement(12);
    ODPActions.clickMacroElement(13);
    ODPLocators.sapClient.sendKeys("${secure(testjcoclient)}");
    ODPLocators.language.sendKeys("${secure(testlang)}");
    ODPLocators.sapApplicationServerHost.sendKeys("${secure(testjcoserver)}");
    ODPLocators.systemNumber.sendKeys("${secure(testjcosysnr)}");
    ODPLocators.dataSourceName.sendKeys("${secure(testjcodatasourcename)}");
    ODPLocators.extractType.sendKeys("${secure(testloadtype)}");
    ODPLocators.usernameCredentials.sendKeys("${secure(testuserqa)}");
    ODPLocators.macroPass.sendKeys("${secure(testpasswordqa)}");
    ODPLocators.gcsProjectID.sendKeys("auto-detect");
    ODPLocators.gcsPath.sendKeys("${secure(testgcspath)}");
    ODPLocators.splitRow.sendKeys("${secure(testjcosplit)}");
    ODPLocators.packageSize.sendKeys("${secure(testjcopackagesize)}");
    ODPLocators.validateButton.click();
    ODPLocators.successMessage.isDisplayed();
  }
}
