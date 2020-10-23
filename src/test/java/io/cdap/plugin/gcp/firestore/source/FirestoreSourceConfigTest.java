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

package io.cdap.plugin.gcp.firestore.source;

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.firestore.util.FirestoreConstants;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

/**
 * Tests for {@link FirestoreSourceConfig}.
 */
public class FirestoreSourceConfigTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testValidateCollectionNull() {
    MockFailureCollector collector = new MockFailureCollector();
    FirestoreSourceConfig config = withFirestoreValidationMock(FirestoreSourceConfigHelper.newConfigBuilder()
      .setCollection(null)
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(FirestoreConstants.PROPERTY_COLLECTION, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testValidateCollectionEmpty() {
    MockFailureCollector collector = new MockFailureCollector();
    FirestoreSourceConfig config = withFirestoreValidationMock(FirestoreSourceConfigHelper.newConfigBuilder()
      .setCollection("")
      .build(), collector);

    config.validate(collector);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals(FirestoreConstants.PROPERTY_COLLECTION, collector.getValidationFailures().get(0)
      .getCauses().get(0).getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Test
  public void testIsIncludeDocumentIdTrue() {
    FirestoreSourceConfig config = FirestoreSourceConfigHelper.newConfigBuilder()
      .setIncludeDocumentId("true")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertTrue(config.isIncludeDocumentId());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testIsIncludeDocumentIdFalse() {
    FirestoreSourceConfig config = FirestoreSourceConfigHelper.newConfigBuilder()
      .setIncludeDocumentId("false")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertFalse(config.isIncludeDocumentId());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testIsIncludeDocumentIdInvalid() {
    FirestoreSourceConfig config = FirestoreSourceConfigHelper.newConfigBuilder()
      .setIncludeDocumentId("invalid")
      .build();

    MockFailureCollector collector = new MockFailureCollector();
    Assert.assertFalse(config.isIncludeDocumentId());
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  private FirestoreSourceConfig withFirestoreValidationMock(FirestoreSourceConfig config, FailureCollector collector) {
    FirestoreSourceConfig spy = Mockito.spy(config);
    Mockito.doNothing().when(spy).validateFirestoreConnection(collector);
    return spy;
  }
}
