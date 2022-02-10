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

package io.cdap.plugin.gcp.bigquery.sqlengine.builder;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.powermock.api.mockito.PowerMockito.spy;

@RunWith(MockitoJUnitRunner.class)
public class BigQuerySelectSQLBuilderTest {

  BigQuerySelectSQLBuilder helper;
  Map<String, String> columns;
  Map<String, String> singleColumn;

  @Before
  public void setUp() {
    // ensure columns are in order
    columns = new LinkedHashMap<>();
    columns.put("a", "a");
    columns.put("b", "function(b)");
    columns.put("c", "d");
    singleColumn = Collections.singletonMap("conCols", "CONCAT(col1, col2)");
  }

  @Test
  public void testBuildSelectedFields() {
    helper = spy(new BigQuerySelectSQLBuilder(columns,
                                              "source-table",
                                              "source-alias",
                                              null));

    String selectedFields = helper.getSelectedFields();
    Assert.assertTrue(selectedFields.contains("a AS `a`"));
    Assert.assertTrue(selectedFields.contains("function(b) AS `b`"));
    Assert.assertTrue(selectedFields.contains("d AS `c`"));
    Assert.assertEquals("a AS `a` , function(b) AS `b` , d AS `c`",
                        selectedFields);
  }

  @Test
  public void testSelect() {
    helper = spy(new BigQuerySelectSQLBuilder(singleColumn,
                                              "source-table",
                                              "source-alias",
                                              null));
    String query = helper.getQuery();
    Assert.assertEquals("SELECT CONCAT(col1, col2) AS `conCols` FROM source-table AS `source-alias`",
                        query);
  }

  @Test
  public void testSelectWithFilter() {
    helper = spy(new BigQuerySelectSQLBuilder(singleColumn,
                                              "source-table",
                                              "source-alias",
                                              "some-col = some-other-col"));
    String query = helper.getQuery();
    Assert.assertEquals("SELECT CONCAT(col1, col2) AS `conCols` FROM source-table AS `source-alias` " +
                          "WHERE some-col = some-other-col",
                        query);
  }


}
