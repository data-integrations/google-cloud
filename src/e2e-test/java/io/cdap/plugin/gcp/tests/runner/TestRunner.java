/**
 * @Copyright
 * This class is the runner class to run the GCS test cases
 */
package io.cdap.plugin.gcp.tests.runner;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;


/**
 * Test Runner to execute cases.
 */
@RunWith(Cucumber.class)
@CucumberOptions(
        features = {"src/e2e-test/java/features"},
        glue = {"io.cdap.plugin.gcp.stepsdesign"},
        monochrome = true,
        plugin = {"pretty", "html:target/cucumber-html-report", "json:target/cucumber-reports/cucumber.json",
          "junit:target/cucumber-reports/cucumber.xml"}
)
public class TestRunner {
}
