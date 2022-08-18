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

package io.cdap.plugin.gcp.common;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;
import org.junit.Assume;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * GCP related information for tests that need to actually interact with GCP APIs.
 */
public class TestEnvironment {
  private static final AtomicReference<TestEnvironment> INSTANCE = new AtomicReference<>(null);
  private final String project;
  private final GoogleCredentials credentials;
  private final String serviceAccountFilePath;
  private final String serviceAccountContent;

  private TestEnvironment(String project, GoogleCredentials credentials,
                          @Nullable String serviceAccountFilePath, @Nullable String serviceAccountContent) {
    this.project = project;
    this.credentials = credentials;
    this.serviceAccountFilePath = serviceAccountFilePath;
    this.serviceAccountContent = serviceAccountContent;
  }

  public String getProject() {
    return project;
  }

  public GoogleCredentials getCredentials() {
    return credentials;
  }

  @Nullable
  public String getServiceAccountFilePath() {
    return serviceAccountFilePath;
  }

  @Nullable
  public String getServiceAccountContent() {
    return serviceAccountContent;
  }

  /**
   * Skips the test if no service account was provided with -Dservice.account.file=[path/to/sa]
   */
  public void skipIfNoServiceAccountGiven() {
    Assume.assumeFalse("Skipping test because no service account was configured. " +
                         "Set it with -Dservice.account.file=[path/to/sa]", serviceAccountContent == null);
  }

  /**
   * Try to load test properties from the environment.
   *
   * First looks at system properties to check if the project or service account information has been passed in.
   * Project should be specified with -Dproject.id and service account path as -Dservice.account.file.
   *
   * If none are specified, the information will attempt to be loaded from the environment. If it is unavailable,
   * tests will be skipped.
   *
   * @return test environment properties
   */
  public static TestEnvironment load() throws IOException {
    if (INSTANCE.get() != null) {
      return INSTANCE.get();
    }

    String project = System.getProperty("project.id");
    if (project == null) {
      project = ServiceOptions.getDefaultProjectId();
    }
    Assume.assumeFalse("Skipping test because no project was configured. Set it with -Dproject.id=[project]",
                       project == null);

    GoogleCredentials credentials = null;
    String serviceAccountFilePath = System.getProperty("service.account.file");
    String serviceAccountContent = null;
    if (serviceAccountFilePath != null) {
      try (InputStream inputStream = new FileInputStream(serviceAccountFilePath)) {
        credentials = GoogleCredentials.fromStream(inputStream);
      }
      Path path = Paths.get(new File(serviceAccountFilePath).getAbsolutePath());
      serviceAccountContent = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    } else {
      try {
        credentials = GoogleCredentials.getApplicationDefault();
      } catch (IOException e) {
        // this is ok, means credentials are not available.
      }
    }
    Assume.assumeFalse("Skipping test because no service account was configured. " +
                         "Set it with -Dservice.account.file=[path/to/sa]",
                       credentials == null);

    TestEnvironment testEnvironment = new TestEnvironment(project, credentials,
                                                          serviceAccountFilePath, serviceAccountContent);
    INSTANCE.compareAndSet(null, testEnvironment);
    return INSTANCE.get();
  }
}
