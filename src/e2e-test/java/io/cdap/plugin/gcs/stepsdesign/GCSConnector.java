package io.cdap.plugin.gcs.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBigQueryPropertiesActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfLogActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfBigQueryPropertiesLocators;
import io.cdap.e2e.pages.locators.CdfGCSLocators;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.utils.CdapUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.remote.RemoteWebDriver;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.Select;
import org.openqa.selenium.support.ui.WebDriverWait;
import stepsdesign.BeforeActions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

import static io.cdap.e2e.utils.RemoteClass.createDriverFromSession;

/**
 * GCSrefactor.
 */
public class GCSConnector implements CdfHelper {

  @Given("Open Datafusion Project to configure pipeline")
  public void openDatafusionProjectToConfigurePipeline() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Source is GCS bucket")
  public void sourceIsGCSBucket() throws InterruptedException {
    CdfStudioActions.selectGCS();
  }

  @When("Target is BigQuery")
  public void targetIsBigQuery() {
    CdfStudioActions.sinkBigQuery();
  }

  @Then("Link Source and Sink to establish connection")
  public void linkSourceAndSinkToEstablishConnection() throws InterruptedException {
    Thread.sleep(2000);
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromGCS, CdfStudioLocators.toBigQiery);
  }

  @Then("Wait till pipeline is in running state_with element")
  public void waitTillPipelineIsInRunningStateWithElement() throws InterruptedException {
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 1000000);
    wait.until(ExpectedConditions.visibilityOfElementLocated(By.xpath("//*[@data-cy='S" +
                                                                        "ucceeded' " +
                                                                        "or @data-cy='Failed']")));
  }

  @Then("Enter the GCS Properties with GCS bucket {string} and format {string}")
  public void enterTheGCSPropertiesWithGCSBucket(String bucket, String format) throws IOException,
    InterruptedException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.clickUseConnection();
    CdfGcsActions.clickBrowseConnection();
    CdfGcsActions.selectConnection(CdapUtils.pluginProp("gcsConnectionName"));
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 50);
    wait.until(ExpectedConditions.visibilityOf(CdfGCSLocators.referenceName));
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterSamplesize();
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp(bucket));
    CdfGcsActions.selectFormat(CdapUtils.pluginProp(format));
    CdfGcsActions.skipHeader();
    CdfGcsActions.getSchema();
    wait.until(ExpectedConditions.
                 invisibilityOfElementLocated(By.xpath("//*[@placeholder='Field name' and @value='offset']")));
  }

  @Then("Enter the GCS Properties with GCS bucket {string} and format {string} by entering blank referenceName")
  public void enterTheGCSPropertiesByPuttingBlankValueInMandatoryFields(String bucket, String format)
    throws IOException, InterruptedException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.clickUseConnection();
    CdfGcsActions.clickBrowseConnection();
    CdfGcsActions.selectConnection(CdapUtils.pluginProp("gcsConnectionName"));
    CdfGcsActions.enterSamplesize();
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp(bucket));
    CdfGcsActions.selectFormat(CdapUtils.pluginProp(format));
    CdfGcsActions.skipHeader();
    CdfGcsActions.getSchema();
  }

  @Then("verify the schema in output")
  public void verifyTheSchemaInOutput() {
    CdfGcsActions.validateSchema();
  }

  @Then("Verify the pipeline status in each case")
  public void verifyThePipelineStatusInFailedCase() {
    WebElement status = SeleniumDriver.getDriver().
      findElement(By.xpath("//*[@data-cy='Succeeded' or @data-cy='Failed']"));
    String str = status.getText();
    Assert.assertEquals(str, "Succeeded");
  }

  @Then("verify the datatype")
  public void verifyTheDatatype() {
    Assert.assertEquals(CdfGcsActions.validateDatatype(), true);
  }

  @Then("Enter the GCS Properties with GCS bucket {string} and format {string} by entering all fields")
  public void enterTheGCSPropertiesWithAllFieldsGCSBucket(String bucket, String format)
    throws IOException, InterruptedException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.clickUseConnection();
    CdfGcsActions.clickBrowseConnection();
    CdfGcsActions.selectConnection(CdapUtils.pluginProp("gcsConnectionName"));
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 30);
    wait.until(ExpectedConditions.
                 invisibilityOfElementLocated(By.xpath("//h5[contains(text(),'Browse Connections')]")));
    CdfGcsActions.enterSamplesize();
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp(bucket));
    CdfGcsActions.selectFormat(CdapUtils.pluginProp(format));
    CdfGcsActions.skipHeader();
    CdfGcsActions.enterMaxsplitsize(CdapUtils.pluginProp("gcsMaxsplitSize"));
    CdfGcsActions.enterMinsplitsize(CdapUtils.pluginProp("gcsMinsplitSize"));
    CdfGcsActions.enterRegexpath(CdapUtils.pluginProp("gcsRegexpath"));
    CdfGcsActions.enterPathfield(CdapUtils.pluginProp("gcspathField"));
    CdfGcsActions.getSchema();
    wait.until(ExpectedConditions.
                 invisibilityOfElementLocated(By.xpath("//*[@placeholder='Field name' and @value='offset']")));
  }

  @Then("Enter the GCS Properties with GCS bucket {string} , format {string} and fileEncoding {int}")
  public void enterTheGCSPropertiesWithUTFGCSBucket(String bucket, String format, int utf)
    throws InterruptedException, IOException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.clickUseConnection();
    CdfGcsActions.clickBrowseConnection();
    CdfGcsActions.selectConnection(CdapUtils.pluginProp("gcsConnectionName"));
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 30);
    wait.until(ExpectedConditions.
                 invisibilityOfElementLocated(By.xpath("//h5[contains(text(),'Browse Connections')]")));
    CdfGcsActions.enterSamplesize();
    CdfGcsActions.enterOverride(CdapUtils.pluginProp("gcsOverride"));
    CdfGcsActions.clickOverrideDatatype(CdapUtils.pluginProp("datatype"));
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp(bucket));
    CdfGcsActions.selectFormat(CdapUtils.pluginProp(format));
    CdfGcsActions.skipHeader();
    CdfGcsActions.selectFileEncoding(utf);
    CdfGcsActions.getSchema();
    wait.until(ExpectedConditions.
                 invisibilityOfElementLocated(By.xpath("//*[@placeholder='Field name' and @value='offset']")));
  }

  @Then("Enter the BigQuery Properties for the table {string}")
  public void enterTheBigQueryProperties(String tableName) throws IOException, InterruptedException {
    CdfBigQueryPropertiesActions.enterbigQueryProperties();
    CdfBigQueryPropertiesActions.enterBigQueryReferenceName(CdapUtils.pluginProp("gcsBqRefName"));
    CdfBigQueryPropertiesActions.enterProjectId(CdapUtils.pluginProp("ProjectId"));
    CdfBigQueryPropertiesActions.enterBigqueryDataSet(CdapUtils.pluginProp("dataset"));
    CdfBigQueryPropertiesActions.enterbigQueryTable(CdapUtils.pluginProp(tableName));
    CdfBigQueryPropertiesActions.clickUpdateTable();
    CdfBigQueryPropertiesActions.clickTruncatableSwitch();
    CdfBigQueryPropertiesActions.clickValidateBttn();
    SeleniumHelper.waitElementIsVisible(CdfBigQueryPropertiesLocators.textSuccess, 1L);
  }

  @Then("Save and Deploy Pipeline")
  public void saveAndDeployPipeline() throws InterruptedException {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("TestPipeline" + UUID.randomUUID().toString());
    CdfStudioActions.pipelineSave();
    Thread.sleep(3000);
    CdfStudioActions.pipelineDeploy();
  }

  @Then("Run the Pipeline in Runtime")
  public void runThePipelineInRuntime() throws InterruptedException {
    CdfPipelineRunAction.runClick();
  }

  @Then("Wait till pipeline is in running state")
  public void waitTillPipelineIsInRunningState() throws InterruptedException {
    Boolean bool = true;
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 1000000);
    wait.until(ExpectedConditions.or(ExpectedConditions.
                                       visibilityOfElementLocated(By.xpath("//*[@data-cy='Succeeded']")),
                                     ExpectedConditions.
                                       visibilityOfElementLocated(By.xpath("//*[@data-cy='Failed']"))));
  }

  @Then("Verify the pipeline status is {string}")
  public void verifyThePipelineStatusIs(String status) {
    boolean webelement = false;
    webelement = SeleniumHelper.verifyElementPresent("//*[@data-cy='" + status + "']");
    Assert.assertTrue(webelement);
  }

  @Then("Open Logs")
  public void openLogs() throws FileNotFoundException, InterruptedException {
    CdfPipelineRunAction.logsClick();
    BeforeActions.scenario.write(CdfPipelineRunAction.captureRawLogs());
    PrintWriter out = new PrintWriter(BeforeActions.myObj);
    out.println(CdfPipelineRunAction.captureRawLogs());
    out.close();
  }

  @Then("validate successMessage is displayed")
  public void validateSuccessMessageIsDisplayed() {
    CdfLogActions.validateSucceeded();
  }

  @Then("Click on Advance logs and validate the success message")
  public void clickOnAdvanceLogsAndValidateTheSuccessMessage() {
    CdfLogActions.goToAdvanceLogs();
    CdfLogActions.validateSucceeded();
  }

  @Then("Verify reference name validation")
  public void verifyReferenceNameValidation() {
    String expectedErrorMessage = CdapUtils.errorProp("errorMessageReference");
    String actualErrorMessage = CdfGCSLocators.referenceError.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    CdfGcsActions.getReferenceErrorColor();
  }

  @Then("Verify path validation")
  public void verifyPathValidation() {
    String expectedErrorMessage = CdapUtils.errorProp("errorMessagePath");
    String actualErrorMessage = CdfGCSLocators.pathError.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    CdfGcsActions.getPathErrorColor();

  }

  @Then("Close the GCS Properties")
  public void closeTheGCSProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Close the BigQuery Properties")
  public void closeTheBigQueryProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Enter the GCS Properties with GCS bucket and format {string} by entering blank path")
  public void enterTheGCSPropertiesWithGCSBucketAndFormatByEnteringBlankPath(String format)
    throws InterruptedException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.clickUseConnection();
    CdfGcsActions.clickBrowseConnection();
    CdfGcsActions.selectConnection(CdapUtils.pluginProp("gcsConnectionName"));
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterSamplesize();
    CdfGcsActions.selectFormat(CdapUtils.pluginProp(format));
    CdfGcsActions.skipHeader();
    CdfGcsActions.getSchema();

  }
}




