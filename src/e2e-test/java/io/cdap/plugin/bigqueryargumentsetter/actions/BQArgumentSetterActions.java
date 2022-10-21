/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.plugin.bigqueryargumentsetter.actions;

import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.plugin.bigqueryargumentsetter.locators.BQArgumentSetterLocators;
import io.cucumber.core.logging.Logger;
import io.cucumber.core.logging.LoggerFactory;

/**
 * BQArgumentSetter plugin step actions.
 */
public class BQArgumentSetterActions {
    private static final Logger logger = (Logger) LoggerFactory.getLogger(BQArgumentSetterActions.class);

    static {
        SeleniumHelper.getPropertiesLocators(BQArgumentSetterLocators.class);
    }

    public static void enterArgumentColumn(int row, String argumentColumn) {
        ElementHelper.sendKeys(BQArgumentSetterLocators.locateArgumentColumn(row), argumentColumn);
    }

    public static void clickArgumentColumnsAddRowButton(int row) {
        ElementHelper.clickOnElement(BQArgumentSetterLocators.locateArgumentColumnsAddRowButton(row));
    }
}
