package runner;

import org.junit.runner.RunWith;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;


@RunWith(Cucumber.class)
@CucumberOptions(
        features={"src/e2e-test/java/features/DemoGcs.feature"},
        glue= {"stepsDesign"},
        monochrome=true,
        plugin={"pretty","html:target/cucumber-html-report","json:target/cucumber-reports/cucumber.json","junit:target/cucumber-reports/cucumber.xml"}

)


public class TestRunner {
}