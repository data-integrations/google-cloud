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

package io.cdap.plugin.gcp.bigquery.sqlengine.transform;

import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import org.junit.Assert;
import org.junit.Test;

public class SQLEngineAvroToStructuredTransformerTest {

  @Test
  public void mapInteger() {
    Long min = (long) Integer.MIN_VALUE;
    Long max = (long) Integer.MAX_VALUE;
    Assert.assertEquals(Integer.MIN_VALUE, (int) SQLEngineAvroToStructuredTransformer.mapInteger(min));
    Assert.assertEquals(0, (int) SQLEngineAvroToStructuredTransformer.mapInteger(0L));
    Assert.assertEquals(Integer.MAX_VALUE, (int) SQLEngineAvroToStructuredTransformer.mapInteger(max));
  }

  @Test(expected = SQLEngineException.class)
  public void mapintegerBelowRange() {
    Long min = (long) Integer.MIN_VALUE - 1;
    Assert.assertEquals(Integer.MIN_VALUE, (int) SQLEngineAvroToStructuredTransformer.mapInteger(min));
  }

  @Test(expected = SQLEngineException.class)
  public void mapintegerAboveRange() {
    Long max = (long) Integer.MAX_VALUE + 1;
    Assert.assertEquals(Integer.MAX_VALUE, (int) SQLEngineAvroToStructuredTransformer.mapInteger(max));
  }

  @Test
  public void mapFloat() {
    Double min = (double) -Float.MAX_VALUE;
    Double max = (double) Float.MAX_VALUE;
    Assert.assertEquals(-Float.MAX_VALUE, SQLEngineAvroToStructuredTransformer.mapFloat(min), 1e-38);
    Assert.assertEquals(0, SQLEngineAvroToStructuredTransformer.mapFloat(0D), 1e-38);
    Assert.assertEquals(Float.MAX_VALUE, SQLEngineAvroToStructuredTransformer.mapFloat(max), 1e-38);
  }
}
