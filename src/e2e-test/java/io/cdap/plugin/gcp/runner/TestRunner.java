/**
 * Info about this package doing something for package-info.java file.
 */
package io.cdap.plugin.gcp.runner;
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
