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
package io.cdap.plugin.gcp.bigquery.sink;

import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.JobConfiguration;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryHelper;
import com.google.cloud.hadoop.io.bigquery.output.ForwardingBigQueryFileOutputCommitter;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.FieldSetter;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberMatcher;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.times;
import static org.powermock.api.support.membermodification.MemberModifier.suppress;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BigQueryOutputFormat.BigQueryOutputCommitter.class})
public class BigQueryOutputFormatTest {

  private static List<String> listOfStrings;
  private JobContext jobContextMock;

  public static void generateList(int pathListSize) {
    StringBuilder sb = new StringBuilder();

    for (int i = 1; i <= pathListSize; i++) {
      sb.append("test").append(i).append(",");
    }
    listOfStrings = Arrays.asList(sb.toString().split(","));
  }

  private BigQueryOutputFormat.BigQueryOutputCommitter initMocks(String operation) throws Exception {

    suppress(PowerMockito.constructor(BigQueryOutputFormat.BigQueryOutputCommitter.class));
    suppress(MemberMatcher.methodsDeclaredIn(ForwardingBigQueryFileOutputCommitter.class));

    BigQueryHelper bigQueryHelperMock = PowerMockito.mock(BigQueryHelper.class, Mockito.RETURNS_DEEP_STUBS);
    PowerMockito.when(bigQueryHelperMock.getRawBigquery().datasets().get(ArgumentMatchers.any(), ArgumentMatchers.any())
                        .execute()).thenReturn(new Dataset());

    Configuration conf = new Configuration();
    conf.set("mapred.bq.output.project.id", "test_project");
    conf.set(BigQueryConfiguration.OUTPUT_DATASET_ID.getKey(), "test_dataset");
    conf.set(BigQueryConfiguration.OUTPUT_TABLE_ID.getKey(), "test_table");
    conf.set("mapred.bq.output.gcs.fileformat", BigQueryFileFormat.AVRO.toString());
    conf.set(BigQueryConstants.CONFIG_OPERATION, operation);

    jobContextMock = Mockito.mock(JobContext.class);
    PowerMockito.when(jobContextMock.getConfiguration()).thenReturn(conf);

    BigQueryOutputFormat.BigQueryOutputCommitter bigQueryOutputCommitter =
      new BigQueryOutputFormat.BigQueryOutputCommitter(null, null);

    BigQueryOutputFormat.BigQueryOutputCommitter spy = PowerMockito.spy(bigQueryOutputCommitter);
    PowerMockito.doReturn(listOfStrings).when(spy, "getOutputFileURIs");
    PowerMockito.doNothing().when(spy, "triggerBigqueryJob", ArgumentMatchers.eq("test_project"),
                                  ArgumentMatchers.anyString(),
                                  ArgumentMatchers.any(Dataset.class),
                                  ArgumentMatchers.any(JobConfiguration.class),
                                  ArgumentMatchers.any(TableReference.class));

    PowerMockito.doNothing().when(spy, "handleUpdateUpsertOperation", ArgumentMatchers.any(TableReference.class),
                                  ArgumentMatchers.anyBoolean(),
                                  ArgumentMatchers.any(),
                                  ArgumentMatchers.any(JobId.class),
                                  ArgumentMatchers.any(),
                                  ArgumentMatchers.any(Dataset.class),
                                  ArgumentMatchers.any());

    FieldSetter.setField(spy, bigQueryOutputCommitter.getClass().getDeclaredField("bigQueryHelper"),
                         bigQueryHelperMock);

    return spy;
  }


  @Test
  public void commitJobTestInsertBQInvocations() throws Exception {

    generateList(500);
    BigQueryOutputFormat.BigQueryOutputCommitter bqQueryOutputCommitterSpy = initMocks("INSERT");
    bqQueryOutputCommitterSpy.commitJob(jobContextMock);

    // 3 batches in temp table and 1 batch for table copy
    PowerMockito.verifyPrivate(bqQueryOutputCommitterSpy, times(1))
      .invoke("triggerBigqueryJob", ArgumentMatchers.eq("test_project"),
              ArgumentMatchers.anyString(),
              ArgumentMatchers.any(Dataset.class),
              ArgumentMatchers.any(JobConfiguration.class),
              ArgumentMatchers.any(TableReference.class));
  }

  @Test
  public void commitJobTestInsert10000BQInvocations() throws Exception {

    generateList(20001);
    BigQueryOutputFormat.BigQueryOutputCommitter bqQueryOutputCommitterSpy = initMocks("INSERT");
    bqQueryOutputCommitterSpy.commitJob(jobContextMock);

    // 3 batches in temp table and 1 batch for table copy
    PowerMockito.verifyPrivate(bqQueryOutputCommitterSpy, times(4))
      .invoke("triggerBigqueryJob", ArgumentMatchers.eq("test_project"),
              ArgumentMatchers.anyString(),
              ArgumentMatchers.any(Dataset.class),
              ArgumentMatchers.any(JobConfiguration.class),
              ArgumentMatchers.any(TableReference.class));
  }

  @Test
  public void commitJobTestUpdateBQInvocations() throws Exception {

    generateList(20001);
    BigQueryOutputFormat.BigQueryOutputCommitter bqQueryOutputCommitterSpy = initMocks("UPDATE");
    bqQueryOutputCommitterSpy.commitJob(jobContextMock);

    // 3 batches in temp table
    PowerMockito.verifyPrivate(bqQueryOutputCommitterSpy, times(3))
      .invoke("triggerBigqueryJob", ArgumentMatchers.eq("test_project"),
              ArgumentMatchers.anyString(),
              ArgumentMatchers.any(Dataset.class),
              ArgumentMatchers.any(JobConfiguration.class),
              ArgumentMatchers.any(TableReference.class));

    //1 call to handleUpdateUpsertOperation
    PowerMockito.verifyPrivate(bqQueryOutputCommitterSpy, times(1))
      .invoke("handleUpdateUpsertOperation",
              ArgumentMatchers.any(TableReference.class),
              ArgumentMatchers.anyBoolean(),
              ArgumentMatchers.any(),
              ArgumentMatchers.any(JobId.class),
              ArgumentMatchers.any(),
              ArgumentMatchers.any(Dataset.class),
              ArgumentMatchers.any());
  }

}
