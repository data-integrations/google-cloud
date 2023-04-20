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
package io.cdap.plugin.gcp.publisher.source;

import java.io.Serializable;

/**
 * Class used to configure exponential backoff for Pub/Sub API requests.
 */
public class BackoffConfig implements Serializable {
  final int initialBackoffMs;
  final int maximumBackoffMs;
  final double backoffFactor;

  static final BackoffConfig defaultInstance() {
    return new BackoffConfig(100, 10000, 2.0);
  }

  public BackoffConfig(int initialBackoffMs, int maximumBackoffMs, double backoffFactor) {
    this.initialBackoffMs = initialBackoffMs;
    this.maximumBackoffMs = maximumBackoffMs;
    this.backoffFactor = backoffFactor;
  }

  public int getInitialBackoffMs() {
    return initialBackoffMs;
  }

  public int getMaximumBackoffMs() {
    return maximumBackoffMs;
  }

  public double getBackoffFactor() {
    return backoffFactor;
  }

  /**
   * Builder class for BackoffConfig
   */
  public static class BackoffConfigBuilder implements Serializable {
    public int initialBackoffMs = 100;
    public int maximumBackoffMs = 10000;
    public double backoffFactor = 2.0;

    protected BackoffConfigBuilder() {
    }

    public static BackoffConfigBuilder getInstance() {
      return new BackoffConfigBuilder();
    }

    public BackoffConfig build() {
      if (initialBackoffMs > maximumBackoffMs) {
        throw new IllegalArgumentException("Maximum backoff cannot be smaller than Initial backoff");
      }

      return new BackoffConfig(initialBackoffMs, maximumBackoffMs, backoffFactor);
    }

    public int getInitialBackoffMs() {
      return initialBackoffMs;
    }

    public BackoffConfigBuilder setInitialBackoffMs(int initialBackoffMs) {
      this.initialBackoffMs = initialBackoffMs;
      return this;
    }

    public int getMaximumBackoffMs() {
      return maximumBackoffMs;
    }

    public BackoffConfigBuilder setMaximumBackoffMs(int maximumBackoffMs) {
      this.maximumBackoffMs = maximumBackoffMs;
      return this;
    }

    public double getBackoffFactor() {
      return backoffFactor;
    }

    public BackoffConfigBuilder setBackoffFactor(int backoffFactor) {
      this.backoffFactor = backoffFactor;
      return this;
    }
  }
}
