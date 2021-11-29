package io.cdap.plugin.gcsmultifile.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBQActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfLogActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.gcsmultifile.actions.BigQueryMulifileActions;
import io.cdap.plugin.gcsmultifile.locators.GCSMultifileLocators;
import io.cdap.plugin.gcsmultifile.utils.CdapUtils;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;
import stepsdesign.BeforeActions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

/**
 * GCSmultifileconnector.
 */
public class GCSMultifileConnector implements CdfHelper {

  @Given("Open Datafusion Project to configure pipeline")
  public void openDatafusionProjectToConfigurePipeline() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Source is BigQuery bucket")
  public void sourceIsBigQuery() throws InterruptedException {
    CdfStudioActions.selectBQ();
  }

  @When("Source is GCS bucket")
  public void sourceIsGCSBucket() throws InterruptedException {
    CdfStudioActions.selectGCS();
  }

  @When("Target is GcsMultifile")
  public void targetIsGcsMultifile() {
    BigQueryMulifileActions.selectGcsMultifile();
  }

  @Then("Link Source and Sink to establish connection")
  public void linkSourceAndSinkToEstablishConnection() throws InterruptedException {
    SeleniumHelper.waitElementIsVisible(GCSMultifileLocators.toGcsMultifile);
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromBigQuery, GCSMultifileLocators.toGcsMultifile);
  }

  @Then("Link GCS and GCSMultiFile to establish connection")
  public void linkGCSAndGCSMultiFileToEstablishConnection() throws InterruptedException {
    SeleniumHelper.waitElementIsVisible(GCSMultifileLocators.toGcsMultifile);
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromGCS, GCSMultifileLocators.toGcsMultifile);
  }

  @Then("Enter the BigQuery Properties for table {string} amd dataset {string} for source")
  public void enterTheBigQueryPropertiesForTableForSource(String table, String dataset)
    throws IOException, InterruptedException {
    CdfBQActions.clickBigqueryProperties();
    BigQueryMulifileActions.enterProjectId();
    BigQueryMulifileActions.enterdatasetProjectId();
    CdfBQActions.enterReferenceName();
    CdfBQActions.enterDataset(CdapUtils.pluginProp("dataset"));
    CdfBQActions.enterTable(CdapUtils.pluginProp("multitable"));
    CdfBQActions.getSchema();
    SeleniumHelper.waitElementIsVisible(
      SeleniumDriver.getDriver().findElement(
        By.xpath("//*[@data-cy='get-schema-btn']//span[text()='Get Schema']")));
  }

  @Then("Close the BigQuery Properties")
  public void closeTheBigQueryProperties() {
    CdfBQActions.closeButton();
  }

  @Then("Enter the Gcs Multifile Properties for table {string} and format {string}")
  public void enterTheGcsMultifilePropertiesForTableForSource(String path, String formatType) throws IOException, InterruptedException {
    BigQueryMulifileActions.gcsMultifileProperties();
    BigQueryMulifileActions.enterReferenceName();
    BigQueryMulifileActions.enterProjectId();
    BigQueryMulifileActions.enterGcsMultifilepath(CdapUtils.pluginProp(path));
    BigQueryMulifileActions.selectAllowFlexibleSchema();
    BigQueryMulifileActions.selectFormat(formatType);
  }

  @Then("Close Gcs Multifile Properties")
  public void closeGcsMultifileProperties() {
    BigQueryMulifileActions.closeGcsMultifile();
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

  @Then("Save and Deploy Pipeline")
  public void saveAndDeployPipeline() throws InterruptedException {
    CdfStudioActions.pipelineName();
    CdfStudioActions.pipelineNameIp("TestPipeline" + UUID.randomUUID().toString());
    CdfStudioActions.pipelineSave();
    SeleniumHelper.waitElementIsVisible(GCSMultifileLocators.pipelineSaveSuccessBanner);
    WebDriverWait wait = new WebDriverWait(SeleniumDriver.getDriver(), 5);
    wait.until(ExpectedConditions.invisibilityOf(GCSMultifileLocators.pipelineSaveSuccessBanner));
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

  @Then("Close the GCS Properties")
  public void closeTheGCSProperties() {
    CdfGcsActions.closeButton();
  }

  @Then("Enter the GCS Properties with {string} GCS bucket")
  public void enterTheGCSPropertiesWithGCSBucket(String bucket) throws InterruptedException, IOException {
    CdfGcsActions.gcsProperties();
    CdfGcsActions.enterReferenceName();
    CdfGcsActions.enterProjectId();
    CdfGcsActions.getGcsBucket(CdapUtils.pluginProp(bucket));
    CdfGcsActions.selectFormat("csv");
    CdfGcsActions.skipHeader();
    CdfGcsActions.getSchema();
    SeleniumHelper.waitElementIsVisible(
      SeleniumDriver.getDriver().findElement(
        By.xpath("//*[@data-cy='get-schema-btn']//span[text()='Get Schema']")));
  }

  @Then("Verify reference name validation")
  public void verifyReferenceNameValidation() {
    String expectedErrorMessage = CdapUtils.errorProp("errorMessageReference");
    String actualErrorMessage = GCSMultifileLocators.referenceError.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    BigQueryMulifileActions.getReferenceErrorColor();
  }

  @Then("Verify path validation")
  public void verifyPathValidation() {
    String expectedErrorMessage = CdapUtils.errorProp("errorMessagePath");
    String actualErrorMessage = GCSMultifileLocators.pathError.getText();
    Assert.assertEquals(expectedErrorMessage, actualErrorMessage);
    BigQueryMulifileActions.getPathErrorColor();
  }

  @Then("Click on Source")
  public void clickOnSource() {
    BigQueryMulifileActions.clickSource();
  }
}




