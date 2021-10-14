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
package io.cdap.plugin.utils;

import io.cdap.e2e.utils.SeleniumDriver;
import io.cucumber.core.api.Scenario;
import io.cucumber.java.Before;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This class is a hook to create log file and set up driver.
 */
public class BeforeActions  {
    public static Scenario scenario;
    public static File myObj;
    @Before
    public void setUp(Scenario scenario) throws IOException {
        this.scenario = scenario;
        SeleniumDriver.setUpDriver();
        String[] tab = scenario.getId().split("/");
        int rawFeatureNameLength = tab.length;
        String featureName = tab[rawFeatureNameLength - 1].split(":")[0];
        featureName = featureName.replace(".", "-");
        String scenarioName = scenario.getName().replace(" ", "-").concat(".txt");
        new File("target/e2e-debug/" + featureName).mkdirs();
        myObj = new File("target/e2e-debug/" + featureName + "/" + scenarioName);
        Path path = Paths.get(String.valueOf(myObj));
        if (Files.deleteIfExists(path)) {
            myObj.createNewFile();
        }
        myObj.createNewFile();

    }
}
