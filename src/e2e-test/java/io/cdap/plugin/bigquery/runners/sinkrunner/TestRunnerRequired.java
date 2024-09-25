/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin.bigquery.runners.sinkrunner;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

/**
 * Test Runner to execute only required BigQuery Sink plugin test cases.
 */
@RunWith(Cucumber.class)
@CucumberOptions(
  features = {"src/e2e-test/features"},
  glue = {"io.cdap.plugin.bigquery.stepsdesign", "io.cdap.plugin.gcs.stepsdesign",
    "stepsdesign", "io.cdap.plugin.common.stepsdesign", "io.cdap.plugin.groupby.actions",
    "io.cdap.plugin.groupby.locators", "io.cdap.plugin.groupby.stepsdesign"},
  tags = {"@BigQuery_Sink_Required"},
  monochrome = true,
  //TODO: Enable test once issue is fixed https://cdap.atlassian.net/browse/CDAP-20830
  plugin = {"pretty", "html:target/cucumber-html-report/bigquery-sink-required",
    "json:target/cucumber-reports/cucumber-bigquery-sink-required.json",
    "junit:target/cucumber-reports/cucumber-bigquery-sink-required.xml"}
)
public class TestRunnerRequired {
}
