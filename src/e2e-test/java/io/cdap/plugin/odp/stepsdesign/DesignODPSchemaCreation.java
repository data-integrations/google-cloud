package io.cdap.plugin.odp.stepsdesign;

import io.cdap.plugin.odp.actions.ODPActions;
import io.cdap.plugin.odp.locators.ODPLocators;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.junit.Assert;
import org.openqa.selenium.Keys;

/**
 * DesignODPSchemaCreation.
 */
public class DesignODPSchemaCreation {


  @Then("Validate that schema is created")
  public void validateThatSchemaIsCreated() {
    ODPActions.getSchema();
    ODPLocators.validateButton.click();
    Assert.assertTrue(ODPLocators.successMessage.isDisplayed());
  }

  @When("data source as {string} is added")
  public void dataSourceAsIsAdded(String datasource) throws InterruptedException {

    for (int i = 0; i < 15; i++) {
      ODPLocators.dataSourceName.sendKeys(Keys.BACK_SPACE);
    }
    ODPLocators.dataSourceName.sendKeys(datasource);
    ODPLocators.validateButton.click();
    ODPLocators.successMessage.isDisplayed();
  }
}
