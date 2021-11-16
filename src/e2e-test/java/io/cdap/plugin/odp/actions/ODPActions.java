package io.cdap.plugin.odp.actions;

import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.plugin.odp.locators.ODPLocators;
import io.cdap.plugin.odp.utils.CDAPUtils;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.support.PageFactory;

import java.io.IOException;
import java.util.UUID;

/**
 * ODPActions.
 */
public class ODPActions {
  public static ODPLocators odpLocators = null;

  static {
    odpLocators = PageFactory.initElements(SeleniumDriver.getDriver(), ODPLocators.class);
  }


  public static void selectODPSource() {
    odpLocators.sapODPSource.click();
  }

  public static void getODPProperties() {
    odpLocators.sapODPProperties.click();
  }

  public static void enterDirectConnectionProperties(String client, String sysnr, String asHost, String dsName
    , String gcsPath, String splitRow) throws IOException, InterruptedException {
    odpLocators.referenceName.sendKeys(UUID.randomUUID().toString());
    odpLocators.sapClient.sendKeys(CDAPUtils.pluginProp(client));
    odpLocators.systemNumber.sendKeys(CDAPUtils.pluginProp(sysnr));
    odpLocators.sapApplicationServerHost.sendKeys(CDAPUtils.pluginProp(asHost));
    odpLocators.dataSourceName.sendKeys(CDAPUtils.pluginProp(dsName));
    JavascriptExecutor js = (JavascriptExecutor) SeleniumDriver.getDriver();
    js.executeScript("window.scrollBy(0,350)", "");
    odpLocators.gcsPath.sendKeys(CDAPUtils.pluginProp(gcsPath));
    odpLocators.splitRow.sendKeys(CDAPUtils.pluginProp(splitRow));
  }

  public static void enterLoadConnectionProperties(String client, String asHost, String msServ, String systemID,
                                                   String lgrp, String dsName, String gcsPath, String splitrow,
                                                   String packageSize) throws IOException, InterruptedException {
    odpLocators.referenceName.sendKeys(UUID.randomUUID().toString());
    odpLocators.sapClient.sendKeys(CDAPUtils.pluginProp(client));
    odpLocators.loadServer.click();
    odpLocators.msHost.sendKeys(CDAPUtils.pluginProp(asHost));
    odpLocators.portNumber.sendKeys(CDAPUtils.pluginProp(msServ));
    odpLocators.systemID.sendKeys(CDAPUtils.pluginProp(systemID));
    ODPLocators.logonGroup.sendKeys(CDAPUtils.pluginProp(lgrp));
    odpLocators.dataSourceName.sendKeys(CDAPUtils.pluginProp(dsName));
    JavascriptExecutor js = (JavascriptExecutor) SeleniumDriver.getDriver();
    js.executeScript("window.scrollBy(0,350)", "");
    odpLocators.gcsPath.sendKeys(CDAPUtils.pluginProp(gcsPath));
    odpLocators.splitRow.sendKeys(CDAPUtils.pluginProp(splitrow));
    odpLocators.packageSize.sendKeys(CDAPUtils.pluginProp(packageSize));
  }

  public static void enterUserNamePassword(String username, String password) throws IOException {
    odpLocators.usernameCredentials.sendKeys(username);
    odpLocators.passwordCredentials.sendKeys(password);
  }

  public static void selectSync() {
    odpLocators.syncRadio.click();
  }

  public static void getSchema() {
    odpLocators.getSchemaButton.click();
  }

  public static void closeButton() {
    odpLocators.closeButton.click();
  }

  public static void clickAllMacroElements() {

    int a = odpLocators.macros.size();

    for (int i = 0; i < a - 1; i++) {
      odpLocators.macros.get(i).click();
    }
  }

  public static void clickMacroElement(int i) throws InterruptedException {
    odpLocators.macros.get(i).click();
  }
}
