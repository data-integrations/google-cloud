/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.gcsmove.locators;

import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * GCSDoneFileMarker related locators.
 */
public class GCSDoneFileMarkerLocators {
  public static final String XPATH_PIPELINE_ALERTS_APPLY = "//*[@data-testid='config-apply-close']";

  public static final String  XPATH_GCS_FILE_MARKER_CONFIRM = "//*[@data-cy='confirm-btn']";

  @FindBy(how = How.XPATH, using = "//*[@data-cy='pipeline-configure-modeless-btn']")
  public static WebElement pipelineConfigureButton;

  @FindBy(how = How.XPATH, using = "//*[@data-cy='tab-head-Pipeline alert']")
  public static WebElement pipelineAlertsItem;

  @FindBy(how = How.XPATH, using =  "//*[@data-cy='post-run-alerts-create']")
  public static WebElement pipelineAlertsCreate;

  @FindBy(how = How.XPATH, using = XPATH_PIPELINE_ALERTS_APPLY)
  public static WebElement pipelineAlertsApply;

  @FindBy(how = How.XPATH, using =  "//button[text()='GCSDoneFileMarker']")
  public static WebElement gcsDoneFileMarkerButton;

  @FindBy(how = How.XPATH, using =  "//*[@data-testid='select-runCondition']")
  public static WebElement gcsFileMarkerRunConditionSelect;

  @FindBy(how = How.XPATH, using =  "//*[@data-cy='next-btn']")
  public static WebElement gcsFileMarkerNextBtn;

  @FindBy(how = How.XPATH, using =  XPATH_GCS_FILE_MARKER_CONFIRM)
  public static WebElement gcsFileMarkerConfirmBtn;

}
