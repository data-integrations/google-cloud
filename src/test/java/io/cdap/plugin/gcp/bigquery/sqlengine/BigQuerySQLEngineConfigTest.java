/*
 * Copyright © 2020 Cask Data, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 */

package io.cdap.plugin.gcp.bigquery.sqlengine;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Test for {@link BigQuerySQLEngineConfig} class
 */
public class BigQuerySQLEngineConfigTest {

  @Test
  public void testSplitStages() {
    Set<String> stages = new HashSet<>();

    Assert.assertEquals(stages, BigQuerySQLEngineConfig.splitStages(null));
    Assert.assertEquals(stages, BigQuerySQLEngineConfig.splitStages(""));

    stages.add(" ");
    Assert.assertEquals(stages, BigQuerySQLEngineConfig.splitStages(" "));

    stages.add("a");
    Assert.assertEquals(stages, BigQuerySQLEngineConfig.splitStages(" \u0001a"));

    stages.add("this is some stage");
    Assert.assertEquals(stages, BigQuerySQLEngineConfig.splitStages(" \u0001a\u0001this is some stage"));

    stages.add("  this is another ");
    Assert.assertEquals(stages,
                        BigQuerySQLEngineConfig.splitStages(
                          " \u0001a\u0001this is some stage\u0001  this is another "));
  }
}
