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

package io.cdap.plugin.gcp.publisher.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;

import java.io.Serializable;
import javax.annotation.Nullable;

/**
 * Configuration class for the subscriber source.
 */
public class PubSubSubscriberConfig extends GCPReferenceSourceConfig implements Serializable {

  public static final String NAME_SUBSCRIPTION = "subscription";

  @Description("Cloud Pub/Sub subscription to read from. If a subscription with the specified name does not " +
    "exist, it will be automatically created if a topic is specified. Messages published before the subscription " +
    "was created will not be read.")
  @Macro
  protected String subscription;

  @Description("Cloud Pub/Sub topic to create a subscription on. This is only used when the specified  " +
    "subscription does not already exist and needs to be automatically created. If the specified " +
    "subscription already exists, this value is ignored.")
  @Macro
  @Nullable
  protected String topic;

  @Description("Set the number of readers to run in parallel. There need to be enough workers in the " +
    "cluster to run all receivers. By default, 1 reader is running per Pub/Sub Source.")
  @Macro
  @Nullable
  protected Integer numberOfReaders;

  public void validate(FailureCollector collector) {
    super.validate(collector);

    if (containsMacro(NAME_SUBSCRIPTION)) {
      return;
    }

    String regAllowedChars = "[A-Za-z0-9-.%~+_]*$";
    String regStartWithLetter = "[A-Za-z]";
    if (!getSubscription().matches(regAllowedChars)) {
      collector.addFailure("Subscription Name does not match naming convention.",
                           "Unexpected Character. Check Plugin documentation for naming convention.")
        .withConfigProperty(subscription);
    }
    if (getSubscription().startsWith("goog")) {
      collector.addFailure("Subscription Name does not match naming convention.",
                           " Cannot Start with String goog. Check Plugin documentation for naming convention.")
        .withConfigProperty(subscription);
    }
    if (!getSubscription().substring(0, 1).matches(regStartWithLetter)) {
      collector.addFailure("Subscription Name does not match naming convention.",
                           "Name must start with a letter. Check Plugin documentation for naming convention.")
        .withConfigProperty(subscription);
    }
    if (getSubscription().length() < 3 || getSubscription().length() > 255) {
      collector.addFailure("Subscription Name does not match naming convention.",
                           "Character Length must be between 3 and 255 characters. " +
                             "Check Plugin documentation for naming convention.")
        .withConfigProperty(subscription);
    }
    collector.getOrThrowException();
  }

  public String getSubscription() {
    return subscription;
  }

  public String getTopic() {
    return topic;
  }

  public Integer getNumberOfReaders() {
    return numberOfReaders != null ? numberOfReaders : 1;
  }
}
