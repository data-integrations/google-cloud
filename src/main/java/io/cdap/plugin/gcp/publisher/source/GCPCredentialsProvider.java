/*
 * Copyright © 2019 Cask Data, Inc.
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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.pubsub.PubsubScopes;
import org.apache.spark.streaming.pubsub.SparkGCPCredentials;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nullable;

/**
 * Hack to replace the one in Bahir, which is incompatible with the version of the pubsub library
 * used by this project.
 */
public class GCPCredentialsProvider implements SparkGCPCredentials {
  private final String serviceAccountFilePath;
  private transient Credential credential;

  public GCPCredentialsProvider(@Nullable String serviceAccountFilePath) {
    this.serviceAccountFilePath = serviceAccountFilePath;
  }

  @Override
  public Credential provider() {
    if (credential == null) {
      if (serviceAccountFilePath != null) {
        loadFromFile(serviceAccountFilePath);
      } else {
        try {
          credential = GoogleCredential.getApplicationDefault();
        } catch (IOException e) {
          throw new IllegalArgumentException("Unable to load credentials from environment", e);
        }
      }
    }
    return credential;
  }

  private void loadFromFile(String serviceAccountFilePath) {
    try (InputStream is = new FileInputStream(new File(serviceAccountFilePath))) {
      credential = GoogleCredential.fromStream(is)
        .createScoped(PubsubScopes.all());
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException("Service account file " + serviceAccountFilePath + " does not exist.");
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to load credentials from " + serviceAccountFilePath, e);
    }
  }

  /**
   * Builds credentials.
   */
  public static class Builder {
    private String serviceAccountFilePath;

    public Builder jsonServiceAccount(String serviceAccountFilePath) {
      this.serviceAccountFilePath = serviceAccountFilePath;
      return this;
    }

    public GCPCredentialsProvider build() {
      return new GCPCredentialsProvider(serviceAccountFilePath);
    }
  }
}
