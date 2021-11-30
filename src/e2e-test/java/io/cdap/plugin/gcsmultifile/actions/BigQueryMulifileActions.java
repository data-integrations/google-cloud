package io.cdap.plugin.gcsmultifile.actions;

import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.gcsmultifile.locators.GCSMultifileLocators;
import org.openqa.selenium.By;

import java.io.IOException;
import java.util.UUID;

import static io.cdap.e2e.utils.ConstantsUtil.COLOR;

/**
 * Multifileactions.
 */
public class BigQueryMulifileActions {

  public static GCSMultifileLocators gcsMultifileLocators;

  static {
    gcsMultifileLocators = SeleniumHelper.getPropertiesLocators(GCSMultifileLocators.class);
  }

  public static void enterReferenceName() {
    GCSMultifileLocators.referenceName.sendKeys(UUID.randomUUID().toString());
  }

  public static void enterProjectId() throws IOException {
    SeleniumHelper.replaceElementValue(GCSMultifileLocators.projectId, SeleniumHelper.readParameters("projectId"));
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

  public static String getReferenceNameError() {
    return GCSMultifileLocators.referenceError.getText();
  }

  public static String getReferenceErrorColor() {
    return GCSMultifileLocators.referenceError.getCssValue(COLOR);
  }

  public static String getPathError() {
    return GCSMultifileLocators.pathError.getText();
  }

  public static String getPathErrorColor() {
    return GCSMultifileLocators.pathError.getCssValue(COLOR);
  }

  public static void enterdatasetProjectId() throws IOException {
    SeleniumHelper.replaceElementValue(GCSMultifileLocators.datasetProjectId,
                                       SeleniumHelper.readParameters("datasetProjectId"));
  }

  public static void clickSource() {
    GCSMultifileLocators.source.click();
  }
}
