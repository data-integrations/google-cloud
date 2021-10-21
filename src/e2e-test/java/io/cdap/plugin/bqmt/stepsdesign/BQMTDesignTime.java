package io.cdap.plugin.bqmt.stepsdesign;

import io.cdap.e2e.pages.actions.CdfGcsActions;
import io.cdap.e2e.pages.actions.CdfStudioActions;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.bqmt.actions.CdfBQMTActions;
import io.cdap.plugin.bqmt.locators.CdfBQMTLocators;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;

import java.io.IOException;

/**
 * BQMT DesignTime testcases.
 */
public class BQMTDesignTime implements CdfHelper {


    @Then("Open BQMT Properties")
    public void openBQMTProperties() throws InterruptedException {
        CdfBQMTActions.bqmtProperties();
        Thread.sleep(10000);

    }

    @Then("Verify Reference Field")
    public void verifyReferenceField() throws Exception {
        CdfBQMTActions.referenceNameValidation();
        Thread.sleep(3000);
    }

    @Then("Validate Pipeline")
    public void validatePipeline() throws InterruptedException {
        CdfBQMTActions.clickValidateButton();
        Thread.sleep(10000);
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
        Thread.sleep(5000);
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
        Thread.sleep(2000);
    }

    @Then("Add and Save Comments")//verify comment
    public void addComments() throws InterruptedException, IOException {
        CdfBQMTActions.bqmtProperties();
        Thread.sleep(2000);
        CdfBQMTActions.clickComment();
        CdfBQMTActions.addComment();
        CdfBQMTActions.saveComment();
    }

    @Then("Edit Comments")
    public void editComments() throws InterruptedException, IOException {
        CdfBQMTActions.editComment();
        CdfBQMTActions.clickEdit();
        Thread.sleep(2000);
        CdfBQMTActions.clearComments();
        Thread.sleep(2000);
        CdfBQMTActions.addComment();
        Thread.sleep(2000);
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
        Thread.sleep(2000);
        CdfBQMTActions.clickHamburgerDelete();
    }

    @Then("Validate Comment")
    public void validateComment() throws InterruptedException, IOException {
        CdfBQMTActions.validateComment(SeleniumHelper.readParameters("COMMENT_BQMT"),
                                       CdfBQMTLocators.addComment.getText());
    }
}


