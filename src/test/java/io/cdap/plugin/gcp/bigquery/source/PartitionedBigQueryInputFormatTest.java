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

package io.cdap.plugin.gcp.bigquery.source;

import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Objects;

/**
 *  Unit Tests for generateQuery methods
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BigQueryUtil.class)
public class PartitionedBigQueryInputFormatTest {

  @Test
  public void testGenerateQueryForMaterializingView() {
    String datasetProject = "test_bq_dataset_project";
    String dataset = "test_bq_dataset";
    String table = "test_bq_table";
    String filter = "tableColumn = 'abc'";
    PartitionedBigQueryInputFormat partitionedBigQueryInputFormat = new PartitionedBigQueryInputFormat();
    String generatedQuery = partitionedBigQueryInputFormat.generateQueryForMaterializingView(datasetProject, dataset,
                                                                                             table, filter);
    String expectedQuery = String.format("select * from `%s.%s.%s` where %s", datasetProject, dataset, table, filter);
    Assert.assertEquals(expectedQuery, generatedQuery);

    String expectedQueryWithoutFilter = String.format("select * from `%s.%s.%s`", datasetProject, dataset, table);
    generatedQuery = partitionedBigQueryInputFormat.generateQueryForMaterializingView(datasetProject, dataset,
                                                                                      table, null);
    Assert.assertEquals(expectedQueryWithoutFilter, generatedQuery);
  }

  @Test
  public void testGenerateQuery() {
    String datasetProject = "test_bq_dataset_project";
    String dataset = "test_bq_dataset";
    String table = "test_bq_table";
    String filter = "tableColumn = 'abc'";
    PartitionedBigQueryInputFormat partitionedBigQueryInputFormat = new PartitionedBigQueryInputFormat();
    PowerMockito.mockStatic(BigQueryUtil.class);
    Table t = PowerMockito.mock(Table.class);
    StandardTableDefinition tableDefinition = PowerMockito.mock(StandardTableDefinition.class);
    PowerMockito.when(BigQueryUtil.getBigQueryTable(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                                                    ArgumentMatchers.anyString(), ArgumentMatchers.any(),
                                                    ArgumentMatchers.anyBoolean())).thenReturn(t);
    PowerMockito.when(t.getDefinition()).thenReturn(tableDefinition);
    String generatedQuery = partitionedBigQueryInputFormat.generateQuery(null, null, filter, datasetProject,
                                                                         datasetProject, dataset, table, null, true);
    String expectedQuery = String.format("select * from `%s.%s.%s` where %s", datasetProject, dataset, table, filter);
    Assert.assertEquals(expectedQuery, generatedQuery);

    generatedQuery = partitionedBigQueryInputFormat.generateQuery(null, null, null, datasetProject, datasetProject,
                                                                  dataset, table, null, true);
    Assert.assertNull(generatedQuery);
  }
}
