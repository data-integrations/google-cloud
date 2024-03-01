package io.cdap.plugin.bigtable.runners;


import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

/**
 * Test Runner to execute Bigtable testcases.
 */
@RunWith(Cucumber.class)
@CucumberOptions(
        features = {"src/e2e-test/features"},
        glue = {"io.cdap.plugin.bigtable.stepsdesign", "io.cdap.plugin.common.stepsdesign", "stepsdesign"},
        tags = {"@BigTable"},
        monochrome = true,
        plugin = {"pretty", "html:target/cucumber-html-report/bigtable",
                "json:target/cucumber-reports/cucumber-bigtable.json",
                "junit:target/cucumber-reports/cucumber-bigtable.xml"}
)
public class TestRunner {
}



