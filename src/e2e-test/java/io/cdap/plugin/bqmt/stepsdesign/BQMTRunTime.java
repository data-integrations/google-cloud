package io.cdap.plugin.bqmt.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBQMTActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.pages.locators.CdfStudioLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * BQMT RunTime test cases.
 */
public class BQMTRunTime implements CdfHelper {

  @Given("Open Datafusion Project")
  public void openDatafusionProject() throws IOException, InterruptedException {
    openCdf();
  }

  @When("Source selected is GCS bucket")
  public void sourceSelectedIsGCSBucket() throws InterruptedException {
    CdfHelper.selectSourceGCS();
  }

  @Then("Link GCS to BQMT to establish connection")
  public void linkGCSToBQMTToEstablishConnection() throws InterruptedException {
    Thread.sleep(2000);
    SeleniumHelper.dragAndDrop(CdfStudioLocators.fromGCS, CdfStudioLocators.bigQueryMultiTable);
  }

  @Then("Enter the source GCS Properties with format {string} GCS bucket {string}")
  public void enterTheSourceGCSPropertiesWithFormatGCSBucket(String bucket, String formatType)
    throws IOException, InterruptedException {
    gcsPropertiesWithBucket(bucket, formatType);
  }

  @Then("Enter the GCS format with {string} GCS bucket")
  public void enterTheGCSFormatWithGCSBucket(String format) throws IOException {
    CdfGcsActions.selectFormat(format);
    String expectedString = "delimited";
    String actualString = SeleniumHelper.readParameters(format);
    if (expectedString.equalsIgnoreCase(actualString)) {
      CdfGcsActions.delimiter();
    }
    CdfGcsActions.skipHeader();
    CdfGcsActions.getSchema();

  }

  @Then("Verify the get schema status")
  public void verifyTheGetSchemaStatus() throws InterruptedException {
    SeleniumHelper.waitForParticularTime(10000);
    CdfGcsActions.clickValidateButton();
    CdfPipelineRunAction.schemaStatusValidation();
  }

  @Then("Validate the Schema")//add
  public void validateTheSchema() throws InterruptedException {
    CdfGcsActions.schemaValidation();
    CdfGcsActions.assertionverification();
    SeleniumHelper.waitForParticularTime(5000);
  }

  @Then("Verify the Connector status")
  public void verifyTheConnectorStatus() {
    CdfGcsActions.validateSuccessMessage();
  }

  @Then("Close the Properties of GCS")
  public void closeThePropertiesOfGCS() {
    CdfGcsActions.closeButton();
  }

  @Then("Enter the BQMT Properties")
  public void enterTheBQMTProperties() throws InterruptedException, IOException {
    CdfBQMTActions.bqmtProperties();
    SeleniumHelper.waitForParticularTime(1000);
    CdfBQMTActions.enterReferenceName();
    CdfBQMTActions.enterDataset();
    CdfBQMTActions.truncateTable();
    CdfBQMTActions.temporaryBucketName();
    CdfBQMTActions.allowFlexibleSchemaInOutput();
    CdfBQMTActions.setTableSchemaTrue();
    CdfGcsActions.clickValidateButton();
    SeleniumHelper.waitForParticularTime(5000);
  }

  @Then("Save and Deploy Pipeline of GCS to BQMT")
  public void saveAndDeployPipelineOfGCSToBQMT() throws InterruptedException {
    saveAndDeployPipeline();
  }

  @Then("Run the Pipeline in Runtime  to transfer record")
  public void runThePipelineInRuntimeToTransferRecord() throws InterruptedException {
    runThePipelineInRuntime();
  }

  @Then("Wait till pipeline run")
  public void waitTillPipe() throws InterruptedException {
    waitTillPipelineToComplete();
  }

  @Then("Verify the pipeline status is {string} for the pipeline")
  public void verifyThePipelineStatus(String status) throws InterruptedException {
    verifyThePipelineStatusIsForTheCurrentPipeline(status);

  }

  @Then("Open and capture Logs")
  public void openAndCaptureLogs() throws FileNotFoundException {
    captureLogs();
  }


  @Then("Get Count of no of records transferred to BigQuery {string} {string} {string}")
  public void getCountOfNoOfRecordsTransferredToBigQuery(String table1, String table2, String table3)
    throws IOException, InterruptedException {
    int countTable1 = CdfHelper.getCountOfNoOfRecordsTransferredToBigQueryIn(table1);
    int countTable2 = CdfHelper.getCountOfNoOfRecordsTransferredToBigQueryIn(table2);
    int countTable3 = CdfHelper.getCountOfNoOfRecordsTransferredToBigQueryIn(table3);
    int countRecords = countTable1 + countTable2 + countTable3;
    Assert.assertTrue(countRecords > 0);
  }

  @Then("Delete the BQMT table {string}")
  public void deleteTheBQMTTable(String table) {
  }
}
