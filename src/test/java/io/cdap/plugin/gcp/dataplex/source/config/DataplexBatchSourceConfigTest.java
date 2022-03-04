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

package io.cdap.plugin.gcp.dataplex.source.config;

import io.cdap.cdap.etl.mock.validation.MockFailureCollector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

/**
 * Tests for DataplexBatchSourceConfig
 */
@RunWith(MockitoJUnitRunner.class)
public class DataplexBatchSourceConfigTest {

  @InjectMocks
  DataplexBatchSourceConfig dataplexBatchSourceConfig;

  @Test
  public void testGetProject() {
    assertThrows(IllegalArgumentException.class, () -> (dataplexBatchSourceConfig).getProject());
  }

  @Test
  public void testTryGetProject() {
    assertNull((dataplexBatchSourceConfig).tryGetProject());
  }

  @Test
  public void testGetServiceAccountType() {
    assertNull((dataplexBatchSourceConfig).getServiceAccountType());
  }

  @Test
  public void testGetPartitionFrom() {
    assertNull((dataplexBatchSourceConfig).getPartitionFrom());
  }

  @Test
  public void testGetPartitionTo() {
    assertNull((dataplexBatchSourceConfig).getPartitionTo());
  }

  @Test
  public void testGetFilter() {
    assertNull((dataplexBatchSourceConfig).getFilter());
  }

  @Test
  public void testIsServiceAccountFilePath() {
    assertNull((dataplexBatchSourceConfig).isServiceAccountFilePath());
  }

  @Test
  public void testGetSchema() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    assertNull(dataplexBatchSourceConfig.getSchema(mockFailureCollector));
  }

  @Test
  public void testValidateTable() {
    MockFailureCollector mockFailureCollector = new MockFailureCollector("Stage Name");
    dataplexBatchSourceConfig = DataplexBatchSourceConfig.builder()
      .setReferenceName("test").build();
    try {
      dataplexBatchSourceConfig.validateBigQueryDataset(mockFailureCollector, "project", "dataset", "table-wrong");
    } catch (Exception e) {
    }
    assertEquals(1, mockFailureCollector.getValidationFailures().size());
  }
}

