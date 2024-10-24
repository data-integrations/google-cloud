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
package io.cdap.plugin.common.runners.common;

import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.junit.runner.RunWith;

/**
 * Retry Test Runner to execute failed scenarios test cases.
 */
@RunWith(Cucumber.class)
@CucumberOptions(
  features = {"@target/failed_scenarios.txt"},  // This reruns only failed scenarios,
  glue = {"io.cdap.plugin.gcs.stepsdesign", "io.cdap.plugin.bigquery.stepsdesign", "stepsdesign",
    "io.cdap.plugin.common.stepsdesign", "io.cdap.plugin.pubsub.stepsdesign",
    "io.cdap.plugin.gcsmove.stepsdesign", "io.cdap.plugin.spanner.stepsdesign",
    "io.cdap.plugin.gcsdelete.stepsdesign"},
  monochrome = true,
  plugin = {"pretty", "json:target/cucumber-reports/failed-scenarios.json"}
)
public class RetryTestRunner {
}
