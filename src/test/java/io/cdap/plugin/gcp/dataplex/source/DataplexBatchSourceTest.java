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

package io.cdap.plugin.gcp.dataplex.source;

import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.dataplex.v1.Entity;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.common.GCPConnectorConfig;
import io.cdap.plugin.gcp.dataplex.source.config.DataplexBatchSourceConfig;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.powermock.api.mockito.PowerMockito;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for DataplexBatchSource
 */
@RunWith(MockitoJUnitRunner.class)
//@PrepareForTest({DataplexBatchSource.class, DataplexBatchSourceConfig.class, BigQueryUtil.class})
public class DataplexBatchSourceTest {
  @Mock
  private GCPConnectorConfig connection;

  @Mock
  StatusCode statusCode;

  private DataplexBatchSourceConfig.Builder getBuilder() {
    return DataplexBatchSourceConfig.builder()
      .setReferenceName("test");
  }

  @Test
  public void tryGetProjectTest() {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig config = spy(builder.build());
    when(config.tryGetProject()).thenReturn(null);
    assertNull(config.tryGetProject());
    verify(config, times(1)).tryGetProject();
  }

  @Test
  public void validateEntityConfigWhenLakeIsNull() throws IOException {
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GoogleCredentials credentials = PowerMockito.mock(GoogleCredentials.class);
    DataplexBatchSourceConfig dataplexBatchSourceConfig =
      PowerMockito.spy(builder.setLake(null).setLocation("test").build());
    doReturn("project").when(dataplexBatchSourceConfig).tryGetProject();
    Entity entity = dataplexBatchSourceConfig.getAndValidateEntityConfiguration(mockFailureCollector, credentials);
    assertEquals(null, entity);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccount() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    GoogleCredentials credentials = PowerMockito.mock(GoogleCredentials.class);
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig config = spy(builder.setConnection(connection).build());
    doReturn(null).when(config).getCredentials(mockFailureCollector);
    try {
      config.validateAndGetServiceAccountCredentials(mockFailureCollector);
    } catch (Exception e) {
    }
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }

  // for auto-detect case
  @Test
  public void validateServiceAccountWhenIsServiceAccountJsonFalse() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig config = spy(builder.setConnection(connection).build());
    when(connection.isServiceAccountJson()).thenReturn(Boolean.FALSE);
    when(connection.getServiceAccountFilePath()).thenReturn(null);
    try {
      config.validateAndGetServiceAccountCredentials(mockFailureCollector);
    } catch (Exception e) {
    }
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  // for auto-detect case
  @Test
  public void validateServiceAccountWhenServiceAccountFilePathIsNull() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    when(connection.getServiceAccountFilePath()).thenReturn(null);
    DataplexBatchSourceConfig config = spy(builder.setConnection(connection).build());
    try {
      config.validateAndGetServiceAccountCredentials(mockFailureCollector);
    } catch (Exception e) {
    }
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountIsServiceAccountJsonTrue() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig config = spy(builder.setConnection(connection).build());
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    doReturn(googleCredentials).when(config).getCredentials(mockFailureCollector);
    config.validateAndGetServiceAccountCredentials(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }

  @Test
  public void validateServiceAccountWhenIsServiceAccountJsonTrueAndFilePathIsNotNull() throws Exception {
    MockFailureCollector mockFailureCollector = new MockFailureCollector();
    DataplexBatchSourceConfig.Builder builder = getBuilder();
    DataplexBatchSourceConfig config = spy(builder.setConnection(connection).build());
    GoogleCredentials googleCredentials = PowerMockito.mock(GoogleCredentials.class);
    doReturn(googleCredentials).when(config).getCredentials(mockFailureCollector);
    config.validateAndGetServiceAccountCredentials(mockFailureCollector);
    assertEquals(0, mockFailureCollector.getValidationFailures().size());
  }
}
