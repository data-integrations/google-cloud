package io.cdap.plugin.gcsmultifile.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * multifilelocators.
 */
public class GCSMultifileLocators {

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"referenceName\" and @class=\"MuiInputBase-input\"]")
  public static WebElement referenceName;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"project\" and @class=\"MuiInputBase-input\"]")
  public static WebElement projectId;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"path\" and @class=\"MuiInputBase-input\"]")
  public static WebElement pathField;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"suffix\" and @class=\"MuiInputBase-input\"]")
  public static WebElement pathSuffix;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"suffix\" and @class=\"MuiInputBase-input\"]")
  public static WebElement splitField;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"toggle-No\"]")
  public static WebElement allowFlexible;

  @FindBy(how = How.XPATH, using = "//*[@data-cy=\"select-format\"]")
  public static WebElement format;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='plugin-GCSMultiFiles-batchsink']")
  public static WebElement gcsMultiFileObject;

  @FindBy(how = How.XPATH, using = "//*[@title='GCS Multi File']")
  public static WebElement toGcsMultifile;

  @FindBy(how = How.XPATH, using = "//*[@title='GCS Multi File']//following-sibling::div")
  public static WebElement gcsMultifileProperties;

  @FindBy(how = How.XPATH, using = "//*[@class='fa fa-remove']")
  public static WebElement closeButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'referenceName')]")
  public static WebElement referenceError;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='property-row-error' and contains(text(),'path')]")
  public static WebElement pathError;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='datasetProject' and @class='MuiInputBase-input']")
  public static WebElement datasetProjectId;

  @FindBy(how = How.XPATH, using = "//*[text()='Source ']")
  public static WebElement source;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='valium-banner-hydrator']")
  public static WebElement pipelineSaveSuccessBanner;
}



