package io.cdap.plugin.bqmt.stepsdesign;

import io.cdap.e2e.pages.actions.CdfBQMTActions;
import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.pages.locators.CdfBQMTLocators;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;

import java.io.IOException;

/**
 * BQMT DesignTime testcases.
 */
public class BQMTDesignTime implements CdfHelper {

    @When("Target is BigQueryMultiTable")
    public void targetIsBigQueryMultiTable() {
        CdfStudioActions.sinkBigQueryMultiTable();
    }

    @Then("Open BQMT Properties")
    public void openBQMTProperties() throws InterruptedException {
        CdfBQMTActions.bqmtProperties();
        SeleniumHelper.waitForParticularTime(10000);

    }

    @Then("Verify Reference Field")
    public void verifyReferenceField() throws Exception {
        CdfBQMTActions.referenceNameValidation();
        SeleniumHelper.waitForParticularTime(3000);
    }

    @Then("Validate Pipeline")
    public void validatePipeline() throws InterruptedException {
        CdfBQMTActions.clickValidateButton();
        SeleniumHelper.waitForParticularTime(10000);
    }


    @Then("Close the BQMT Properties")
    public void closeTheBQMTProperties() {
        CdfGcsActions.closeButton();
    }

    @Then("Verify DataSet Field")
    public void verifyDataSetField() throws Exception {
        CdfBQMTActions.dataSetValidation();
    }

    @Then("Enter Reference Name & DataSet Fields with Invalid Test Data")
    public void enterTheBQMTwithInvalidTestData() throws InterruptedException {

        CdfBQMTActions.bqmtProperties();
            CdfBQMTLocators.bqmtReferenceName.sendKeys("#@#@#@#@#@");
            CdfBQMTLocators.bqmtDataSet.sendKeys("$#$#$#$#$#");

        CdfBQMTActions.clickValidateButton();
        SeleniumHelper.waitForParticularTime(5);
    }
    @Then("Verify Reference Name & DataSet Fields with Invalid Test Data")
    public void verifyTheBQMTwithInvalidTestData() throws InterruptedException {
        Assert.assertTrue(CdfBQMTLocators.bqmtReferencenameError.isDisplayed());
        Assert.assertTrue(CdfBQMTLocators.bqmtDataSetError.isDisplayed());
    }

    @Then("Enter Temporary Bucket Field with Invalid Test Data and Verify the Error Message")
    public void enterTheBQMTTemporaryBucketwithInvalidTestData() throws Exception {

        CdfBQMTActions.bqmtProperties();
        CdfBQMTActions.enterReferenceName();
        CdfBQMTActions.enterDataset();
        CdfBQMTLocators.bqmtTemporaryBucketName.sendKeys("#,#@#@#@#@");
        CdfBQMTActions.clickValidateButton();
        CdfBQMTActions.temporaryBucketNameValidation();
        SeleniumHelper.waitForParticularTime(2);
    }

    @Then("Add and Save Comments")//verify comment
    public void addComments() throws InterruptedException, IOException {
        CdfBQMTActions.bqmtProperties();
        SeleniumHelper.waitForParticularTime(2);
        CdfBQMTActions.clickComment();
        CdfBQMTActions.addComment();
        CdfBQMTActions.saveComment();
    }

    @Then("Edit Comments")
    public void editComments() throws InterruptedException, IOException {
        CdfBQMTActions.editComment();
        CdfBQMTActions.clickEdit();
        SeleniumHelper.waitForParticularTime(2);
        CdfBQMTActions.clearComments();
        SeleniumHelper.waitForParticularTime(2);
        CdfBQMTActions.addComment();
        SeleniumHelper.waitForParticularTime(2);
        CdfBQMTActions.saveComment();
    }

    @Then("Delete Comments")//verify
    public void deleteComments() throws InterruptedException {
        CdfBQMTActions.editComment();
        CdfBQMTActions.clickDelete();
    }

    @Then("Click on Hamburger menu and select delete")//verify
    public void hamburgermenu() throws InterruptedException {
        CdfBQMTActions.clickHamburgerMenu();
        SeleniumHelper.waitForParticularTime(2);
        CdfBQMTActions.clickHamburgerDelete();
    }

    @Then("Validate Comment")
    public void validateComment() throws InterruptedException, IOException {
        CdfBQMTActions.validateComment(SeleniumHelper.readParameters("COMMENT_BQMT"),
                                       CdfBQMTLocators.addComment.getText());
    }
}


