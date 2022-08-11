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

import io.cdap.cdap.api.data.schema.Schema;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.spy;
import static org.powermock.api.mockito.PowerMockito.when;

public class DelegatingMultiSinkOutputCommitterTest {

  DelegatingMultiSinkOutputCommitter committer;
  TaskAttemptContext ctx;
  OutputCommitter c1;
  OutputCommitter c2;
  OutputCommitter c3;
  Schema s1;
  Schema s2;
  Schema s3;

  @Before
  @Test
  public void setUp() throws IOException, InterruptedException {
    committer = spy(new DelegatingMultiSinkOutputCommitter("project", "ds", "bucket", "path"));
    doNothing().when(committer).configureContext(any(), anyString());
    ctx = mock(TaskAttemptContext.class);
    c1 = mock(OutputCommitter.class);
    c2 = mock(OutputCommitter.class);
    c3 = mock(OutputCommitter.class);
    s1 = null;
    s2 = null;
    s3 = null;
  }

  @Test
  public void testAddCommitterAndSchema() throws IOException, InterruptedException {
    committer.addCommitterAndSchema(c1, "table1", s1, ctx);

    verify(c1, times(1)).setupJob(ctx);
    verify(c1, times(1)).setupTask(ctx);
  }

  @Test
  public void testNeedsTaskCommit() throws IOException, InterruptedException {
    committer.addCommitterAndSchema(c1, "table1", s1, ctx);
    committer.addCommitterAndSchema(c2, "table2", s2, ctx);
    committer.addCommitterAndSchema(c3, "table3", s3, ctx);

    when(c1.needsTaskCommit(ctx)).thenReturn(false);
    when(c2.needsTaskCommit(ctx)).thenReturn(false);
    when(c3.needsTaskCommit(ctx)).thenReturn(false);

    Assert.assertFalse(committer.needsTaskCommit(ctx));

    when(c1.needsTaskCommit(ctx)).thenReturn(false);
    when(c2.needsTaskCommit(ctx)).thenReturn(false);
    when(c3.needsTaskCommit(ctx)).thenReturn(true);

    Assert.assertFalse(committer.needsTaskCommit(ctx));

    when(c1.needsTaskCommit(ctx)).thenReturn(true);
    when(c2.needsTaskCommit(ctx)).thenReturn(true);
    when(c3.needsTaskCommit(ctx)).thenReturn(true);

    Assert.assertTrue(committer.needsTaskCommit(ctx));
  }

  @Test
  public void testCommitTask() throws IOException, InterruptedException {
    committer.addCommitterAndSchema(c1, "table1", s1, ctx);
    committer.addCommitterAndSchema(c2, "table2", s2, ctx);
    committer.addCommitterAndSchema(c3, "table3", s3, ctx);

    committer.commitTask(ctx);

    verify(c1, times(1)).commitTask(ctx);
    verify(c2, times(1)).commitTask(ctx);
    verify(c3, times(1)).commitTask(ctx);
  }

  @Test
  public void testCommitJob() throws IOException, InterruptedException {
    committer.addCommitterAndSchema(c1, "table1", s1, ctx);
    committer.addCommitterAndSchema(c2, "table2", s2, ctx);
    committer.addCommitterAndSchema(c3, "table3", s3, ctx);

    committer.commitJob(ctx);

    verify(c1, times(1)).commitJob(ctx);
    verify(c2, times(1)).commitJob(ctx);
    verify(c3, times(1)).commitJob(ctx);
  }

  @Test
  public void testAbortTask() throws IOException, InterruptedException {
    committer.addCommitterAndSchema(c1, "table1", s1, ctx);
    committer.addCommitterAndSchema(c2, "table2", s2, ctx);
    committer.addCommitterAndSchema(c3, "table3", s3, ctx);

    committer.abortTask(ctx);

    verify(c1, times(1)).abortTask(ctx);
    verify(c2, times(1)).abortTask(ctx);
    verify(c3, times(1)).abortTask(ctx);
  }

  @Test
  public void testAbortTaskCollectsExceptions() throws IOException, InterruptedException {
    committer.addCommitterAndSchema(c1, "table1", s1, ctx);
    committer.addCommitterAndSchema(c2, "table2", s2, ctx);
    committer.addCommitterAndSchema(c3, "table3", s3, ctx);

    doThrow(new IOException("e1")).when(c1).abortTask(any());
    doThrow(new IOException("e2")).when(c2).abortTask(any());
    doThrow(new IOException("e3")).when(c3).abortTask(any());

    IOException expected = null;
    String message = null;

    try {
      committer.abortTask(ctx);
    } catch (IOException ex) {
      expected = ex;
    }

    Set<String> exceptionMessages = new HashSet<String>() {{
      add("e1");
      add("e2");
      add("e3");
    }};

    Assert.assertNotNull(expected);
    message = expected.getMessage();
    Assert.assertTrue(exceptionMessages.contains(message));
    exceptionMessages.remove(message);

    Assert.assertEquals(2, expected.getSuppressed().length);

    message = expected.getSuppressed()[0].getMessage();
    Assert.assertTrue(exceptionMessages.contains(message));
    exceptionMessages.remove(message);

    message = expected.getSuppressed()[1].getMessage();
    Assert.assertTrue(exceptionMessages.contains(message));
    exceptionMessages.remove(message);
  }

  @Test
  public void testAbortJob() throws IOException, InterruptedException {
    committer.addCommitterAndSchema(c1, "table1", s1, ctx);
    committer.addCommitterAndSchema(c2, "table2", s2, ctx);
    committer.addCommitterAndSchema(c3, "table3", s3, ctx);

    committer.abortJob(ctx, JobStatus.State.FAILED);

    verify(c1, times(1)).abortJob(ctx, JobStatus.State.FAILED);
    verify(c2, times(1)).abortJob(ctx, JobStatus.State.FAILED);
    verify(c3, times(1)).abortJob(ctx, JobStatus.State.FAILED);
  }

  @Test
  public void testAbortJobCollectsExceptions() throws IOException, InterruptedException {
    committer.addCommitterAndSchema(c1, "table1", s1, ctx);
    committer.addCommitterAndSchema(c2, "table2", s2, ctx);
    committer.addCommitterAndSchema(c3, "table3", s3, ctx);

    doThrow(new IOException("e1")).when(c1).abortJob(ctx, JobStatus.State.FAILED);
    doThrow(new IOException("e2")).when(c2).abortJob(ctx, JobStatus.State.FAILED);
    doThrow(new IOException("e3")).when(c3).abortJob(ctx, JobStatus.State.FAILED);

    IOException expected = null;
    String message = null;

    try {
      committer.abortJob(ctx, JobStatus.State.FAILED);
    } catch (IOException ex) {
      expected = ex;
    }

    Set<String> exceptionMessages = new HashSet<String>() {{
      add("e1");
      add("e2");
      add("e3");
    }};

    Assert.assertNotNull(expected);
    message = expected.getMessage();
    Assert.assertTrue(exceptionMessages.contains(message));
    exceptionMessages.remove(message);

    Assert.assertEquals(2, expected.getSuppressed().length);

    message = expected.getSuppressed()[0].getMessage();
    Assert.assertTrue(exceptionMessages.contains(message));
    exceptionMessages.remove(message);

    message = expected.getSuppressed()[1].getMessage();
    Assert.assertTrue(exceptionMessages.contains(message));
    exceptionMessages.remove(message);
  }
}
