package io.cdap.plugin.gcp.bigquery.sqlengine;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TimePartitioning;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class BigQueryReadDatasetTest {

  @Mock
  Table sourceTable;

  @Mock
  StandardTableDefinition tableDefinition;

  @Mock
  BigQuerySQLEngineConfig sqlEngineConfig;

  @Mock
  TimePartitioning timePartitioning;

  String datasetProject = "test_bq_dataset_project";
  String dataset = "test_bq_dataset";
  String table = "test_bq_table";
  String filter = "tableColumn = 'abc'";
  String destTable = "test_bq_dest_table";
  List<String> fieldList = Arrays.asList("id_test", "name_test", "place_test");

  BigQueryReadDataset bigQueryReadDataset;

  @Before
  public void init() {
    TableId destTableId = TableId.of(datasetProject, dataset, destTable);
    bigQueryReadDataset = BigQueryReadDataset.getInstance(
      dataset,
      sqlEngineConfig,
      null,
      null,
      destTableId,
      null);
  }

  @Test
  public void testGenerateQueryForMaterializingView() {
    TableId sourceTableId = TableId.of(datasetProject, dataset, table);
    TableId destTableId = TableId.of(datasetProject, dataset, destTable);

    Mockito.when(tableDefinition.getType()).thenReturn(TableDefinition.Type.VIEW);
    Mockito.when(sourceTable.getDefinition()).thenReturn(tableDefinition);
    Mockito.when(sqlEngineConfig.getJobPriority()).thenReturn(QueryJobConfiguration.Priority.BATCH);

    String generatedQuery = bigQueryReadDataset.getQueryBuilder(sourceTable,
                                                                sourceTableId,
                                                                destTableId,
                                                                fieldList,
                                                                filter,
                                                                "2000-01-01", "2000-01-01")
      .build().getQuery();

    String expectedQuery = String.format("SELECT %s FROM `%s.%s.%s` WHERE %s", String.join(",", fieldList),
                                         datasetProject, dataset, table, filter);
    Assert.assertEquals(expectedQuery, generatedQuery);

    //Without Filter
    generatedQuery = bigQueryReadDataset.getQueryBuilder(sourceTable,
                                                         sourceTableId,
                                                         destTableId,
                                                         fieldList,
                                                         null, null, null)
      .build().getQuery();

    expectedQuery = String.format("SELECT %s FROM `%s.%s.%s`", String.join(",", fieldList),
                                  datasetProject, dataset, table);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

  @Test
  public void testGenerateQuery() {
    TableId sourceTableId = TableId.of(datasetProject, dataset, table);
    TableId destTableId = TableId.of(datasetProject, dataset, destTable);

    Mockito.when(tableDefinition.getType()).thenReturn(TableDefinition.Type.TABLE);
    Mockito.when(sourceTable.getDefinition()).thenReturn(tableDefinition);
    Mockito.when(sqlEngineConfig.getJobPriority()).thenReturn(QueryJobConfiguration.Priority.BATCH);

    //When Table is NOT PARTITIONED
    String generatedQuery = bigQueryReadDataset.getQueryBuilder(sourceTable,
                                                                sourceTableId,
                                                                destTableId,
                                                                fieldList,
                                                                filter,
                                                                "2000-01-01", "2000-01-01")
      .build().getQuery();

    String expectedQuery = String.format("SELECT %s FROM `%s.%s.%s` WHERE %s", String.join(",", fieldList),
                                         datasetProject, dataset, table, filter);
    Assert.assertEquals(expectedQuery, generatedQuery);

    //When Table is PARTITIONED
    Mockito.when(tableDefinition.getTimePartitioning()).thenReturn(timePartitioning);

    generatedQuery = bigQueryReadDataset.getQueryBuilder(sourceTable,
                                                         sourceTableId,
                                                         destTableId,
                                                         fieldList,
                                                         filter,
                                                         "2000-01-01", "2000-01-01")
      .build().getQuery();

    String partitionFilter = "TIMESTAMP(`_PARTITIONTIME`) >= TIMESTAMP(\"2000-01-01\") and " +
      "TIMESTAMP(`_PARTITIONTIME`) < TIMESTAMP(\"2000-01-01\")";
    expectedQuery = String.format("SELECT %s FROM `%s.%s.%s` WHERE %s and (%s)", String.join(",", fieldList),
                                  datasetProject, dataset, table, partitionFilter, filter);
    Assert.assertEquals(expectedQuery, generatedQuery);
  }

}
