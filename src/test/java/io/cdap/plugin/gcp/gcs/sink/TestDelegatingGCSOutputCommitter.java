/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.plugin.gcp.gcs.sink;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.MRConstants;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


/**
 * class to test the DelegatingGCSOutputCommitter class
 */
public class TestDelegatingGCSOutputCommitter {
  private static Path outDir = new Path("file:///tmp/output");

  // A random task attempt id for testing.
  private static String attempt = "attempt_200707121733_0001_m_000000_0";
  private static String partFile = "part-m-00000.avro";
  private static TaskAttemptID taskID = TaskAttemptID.forName(attempt);
  private static String key1 = "key1";
  private static String key2 = "key2";
  private String schema = "{\"type\":\"record\",\"name\":\"text\",\"fields\":" +
    "[{\"name\":\"key1\",\"type\":\"string\"}," +
    "{\"name\":\"key2\",\"type\":\"string\"}]}";
  private StructuredRecord record1 = StructuredRecord.builder(Schema.parseJson(schema))
    .set(key1, "abc")
    .set(key2, "val1")
    .build();
  private StructuredRecord record2 = StructuredRecord.builder(Schema.parseJson(schema))
    .set(key1, "abc")
    .set(key2, "record2")
    .build();
  private static final String tableName = "abc";
  private static final String pathSuffix = LocalDate.now().format(DateTimeFormatter.ISO_DATE);

  public TestDelegatingGCSOutputCommitter() throws IOException {
  }

  private void writeOutput(TaskAttemptContext context, DelegatingGCSOutputCommitter committer) throws IOException,
    InterruptedException {
    NullWritable nullWritable = NullWritable.get();
    DelegatingGCSRecordWriter delegatingGCSRecordWriter = new DelegatingGCSRecordWriter(context, key1,
                                                                                        committer);
    try {
      delegatingGCSRecordWriter.write(nullWritable, record1);
      delegatingGCSRecordWriter.write(nullWritable, record2);
      delegatingGCSRecordWriter.write(nullWritable, record2);
      delegatingGCSRecordWriter.write(nullWritable, record1);
    } finally {
      delegatingGCSRecordWriter.close(null);
    }
  }

  private JobConf getConfiguration() {
    JobConf conf = new JobConf();
    FileOutputFormat.setOutputPath(conf, outDir);
    conf.set(JobContext.TASK_ATTEMPT_ID, attempt);
    conf.setInt(MRConstants.APPLICATION_ATTEMPT_ID, 1);
    conf.set("fs.file.impl", RawLocalFileSystem.class.getName());
    conf.setBoolean("fs.file.impl.disable.cache", true);
    conf.set(DelegatingGCSOutputFormat.DELEGATE_CLASS,
             "io.cdap.plugin.format.avro.output.StructuredAvroOutputFormat");
    conf.set(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR, outDir.toString());
    conf.set(DelegatingGCSOutputFormat.PARTITION_FIELD, "key1");
    conf.set(DelegatingGCSOutputFormat.OUTPUT_PATH_SUFFIX, pathSuffix);
    conf.set("avro.schema.output.key", schema);
    return conf;
  }

  private void testRecoveryInternal()
    throws Exception {
    JobConf conf = getConfiguration();
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.
                  FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    DelegatingGCSOutputCommitter committer = new DelegatingGCSOutputCommitter(tContext);
    writeOutput(tContext, committer);
    if (committer.needsTaskCommit(tContext)) {
      committer.commitTask(tContext);
    }


    String tempPath = String.format("%s/%s/%s/%s/%s", outDir.toUri(), tableName, pathSuffix,
                                    "_temporary/1/", taskID.getTaskID());
    Path jobTempDir1 = new Path(tempPath);
    File jtd1 = new File(jobTempDir1.toUri().getPath());
    assertTrue("Version 1 commits to temporary dir " + jtd1, jtd1.exists());
    validateContent(jobTempDir1);

    Assert.assertFalse(committer.isRecoverySupported(jContext));
    FileUtil.fullyDelete(new File(outDir.toUri()));
  }

  @Test
  public void testRecovery() throws Exception {
    testRecoveryInternal();
  }

  private void validateContent(Path dir) throws IOException {
    File fdir = new File(dir.toUri().getPath());
    File expectedFile = new File(fdir, partFile);
    StringBuffer expectedOutput = new StringBuffer();
    expectedOutput.append(record1.get(key1).toString()).append('\t').append(record1.get(key2).toString()).append("\n");
    expectedOutput.append(record2.get(key1).toString()).append('\t').append(record2.get(key2).toString()).append("\n");
    expectedOutput.append(record2.get(key1).toString()).append('\t').append(record2.get(key2).toString()).append("\n");
    expectedOutput.append(record1.get(key1).toString()).append('\t').append(record1.get(key2).toString()).append("\n");
    String output = slurpAvro(expectedFile);
    assertEquals(output, expectedOutput.toString());
  }

  @Test
  public void testCommitterWithFailureV1() throws Exception {
    testCommitterWithFailureInternal(1);
    testCommitterWithFailureInternal(2);
  }

  private void testCommitterWithFailureInternal(int maxAttempts) throws Exception {
    JobConf conf = getConfiguration();
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.
                  FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION, 1);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    DelegatingGCSOutputCommitter committer = new CommitterWithFailedThenSucceed(tContext);
    // write output
    writeOutput(tContext, committer);

    // do commit
    if (committer.needsTaskCommit(tContext)) {
      committer.commitTask(tContext);
    }

    try {
      committer.commitJob(jContext);
      if (maxAttempts <= 1) {
        Assert.fail("Commit successful: wrong behavior for version 1.");
      }
    } catch (IOException e) {
    }

    FileUtil.fullyDelete(new File(outDir.toString()));
  }

  @Test
  public void testCommitterWithDuplicatedCommit() throws Exception {
    testCommitterWithDuplicatedCommitInternal();
  }

  private void testCommitterWithDuplicatedCommitInternal() throws
    Exception {
    JobConf conf = getConfiguration();
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.
                  FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION,
                1);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    DelegatingGCSOutputCommitter committer = new DelegatingGCSOutputCommitter(tContext);
    writeOutput(tContext, committer);
    if (committer.needsTaskCommit(tContext)) {
      committer.commitTask(tContext);
    }
    committer.commitJob(jContext);

    // validate output
    validateContent(new Path(String.format("%s/%s/%s", outDir, tableName, pathSuffix)));

    // commit again
    committer.commitJob(jContext);
    // It will not fail as this time it will not get any output path as we have removed the _temporaryJob directory
    // that contains the partitions file having output path for each table.
    FileUtil.fullyDelete(new File(outDir.toUri()));
  }

  private void testCommitterInternal() throws Exception {
    JobConf conf = getConfiguration();
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.
                  FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION,
                1);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    DelegatingGCSOutputCommitter committer = new DelegatingGCSOutputCommitter(tContext);
    writeOutput(tContext, committer);
    if (committer.needsTaskCommit(tContext)) {
      committer.commitTask(tContext);
    }
    committer.commitJob(jContext);

    // validate output
    validateContent(new Path(String.format("%s/%s/%s", outDir, tableName, pathSuffix)));
    FileUtil.fullyDelete(new File(outDir.toUri()));
  }

  @Test
  public void testCommitter() throws Exception {
    testCommitterInternal();
  }

  @Test
  public void testMapOnlyNoOutput() throws Exception {
    testMapOnlyNoOutputInternal();
  }

  private void testMapOnlyNoOutputInternal() throws Exception {
    JobConf conf = getConfiguration();
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.
                  FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION,
                1);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    DelegatingGCSOutputCommitter committer = new DelegatingGCSOutputCommitter(tContext);
    if (committer.needsTaskCommit(tContext)) {
      committer.commitTask(tContext);
    }
    committer.commitJob(jContext);

    FileUtil.fullyDelete(new File(outDir.toString()));
  }

  private void testAbortInternal()
    throws IOException, InterruptedException {
    JobConf conf = getConfiguration();
    conf.setInt(org.apache.hadoop.mapreduce.lib.output.
                  FileOutputCommitter.FILEOUTPUTCOMMITTER_ALGORITHM_VERSION,
                1);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    DelegatingGCSOutputCommitter committer = new DelegatingGCSOutputCommitter(tContext);

    // write output
    writeOutput(tContext, committer);

    // do abort
    committer.abortTask(tContext);

    File out = new File(outDir.toUri().getPath());
    String workPathDir = String.format("%s/%s/%s/%s/%s", outDir.toUri(), tableName, pathSuffix,
                                       "_temporary/1/", attempt);
    Path workPath = new Path(workPathDir); //temp attemptid path
    File wp = new File(workPath.toUri().getPath());
    File expectedFile = new File(wp, partFile);
    assertFalse("task temp dir still exists", expectedFile.exists());

    committer.abortJob(jContext, JobStatus.State.FAILED);
    expectedFile = new File(out, String.format("%s_%s", FileOutputCommitter.TEMP_DIR_NAME, taskID.getJobID()));
    assertFalse("job temp dir still exists", expectedFile.exists());
    File tablePath = new File(out, String.format("%s/%s", tableName, pathSuffix));
    assertEquals("Output directory not empty", 0, tablePath.listFiles().length);
    FileUtil.fullyDelete(out);
  }

  @Test
  public void testAbort() throws Exception {
    testAbortInternal();
  }

  public static class FakeFileSystem extends RawLocalFileSystem {
    public FakeFileSystem() {
      super();
    }

    public URI getUri() {
      return URI.create("file:///");
    }

    @Override
    public boolean delete(Path p, boolean recursive) throws IOException {
      throw new IOException("fake delete failed");
    }
  }


  private void testFailAbortInternal()
    throws IOException, InterruptedException {
    JobConf conf = getConfiguration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
    conf.setClass("fs.file.impl", FakeFileSystem.class, FileSystem.class);
    JobContext jContext = new JobContextImpl(conf, taskID.getJobID());
    TaskAttemptContext tContext = new TaskAttemptContextImpl(conf, taskID);
    DelegatingGCSOutputCommitter committer = new DelegatingGCSOutputCommitter(tContext);

    // write output
    writeOutput(tContext, committer);

    File jobTmpDir = new File(
      new Path(outDir.toUri().getPath(), tableName + Path.SEPARATOR + pathSuffix + Path.SEPARATOR +
        FileOutputCommitter.TEMP_DIR_NAME + Path.SEPARATOR +
        conf.getInt(MRConstants.APPLICATION_ATTEMPT_ID, 0) +
        Path.SEPARATOR +
        FileOutputCommitter.TEMP_DIR_NAME).toString());
    File taskTmpDir = new File(jobTmpDir, "" + taskID);
    File expectedFile = new File(taskTmpDir, partFile);

    // do abort
    Throwable th = null;
    try {
      committer.abortTask(tContext);
    } catch (IOException ie) {
      th = ie;
    }
    assertNotNull(th);
    assertTrue(th instanceof IOException);
    assertTrue(th.getMessage().contains("fake delete failed"));
    assertTrue(expectedFile + " does not exists", expectedFile.exists());

    th = null;
    try {
      committer.abortJob(jContext, JobStatus.State.FAILED);
    } catch (IOException ie) {
      th = ie;
    }
    assertNotNull(th);
    assertTrue(th instanceof IOException);
    assertTrue(th.getMessage().contains("fake delete failed"));
    assertTrue("job temp dir does not exists", jobTmpDir.exists());
    FileUtil.fullyDelete(new File(outDir.toString()));
  }

  @Test
  public void testFailAbort() throws Exception {
    testFailAbortInternal();
  }

  public static String slurpAvro(File f) throws IOException {
    StringBuffer expectedOutput = new StringBuffer();
    try {
      // Create a DatumReader for reading GenericRecord from Avro file
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
      // Create a DataFileReader for reading Avro file
      try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(f, datumReader)) {

        // Iterate over records in the Avro file
        while (dataFileReader.hasNext()) {
          // Read the next record
          GenericRecord record = dataFileReader.next();
          expectedOutput.append(record.get(key1)).append('\t').append(record.get(key2)).append("\n");
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return expectedOutput.toString();
  }

  /**
   * The class provides a overrided implementation of commitJobInternal which
   * causes the commit failed for the first time then succeed.
   */
  public static class CommitterWithFailedThenSucceed extends
    DelegatingGCSOutputCommitter {

    public CommitterWithFailedThenSucceed(TaskAttemptContext context) throws IOException {
      super(context);
    }

    @Override
    public void commitJob(JobContext context) throws IOException {
      Configuration conf = context.getConfiguration();
      org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter wrapped =
        new CommitterFailedFirst(new Path(conf.get("mapreduce.output.fileoutputformat.outputdir")),
                                 context);
      wrapped.commitJob(context);
    }
  }

  public static class CommitterFailedFirst extends
    org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter {
    boolean firstTimeFail = true;

    public CommitterFailedFirst(Path outputPath,
                                JobContext context) throws IOException {
      super(outputPath, context);
    }

    @Override
    protected void commitJobInternal(org.apache.hadoop.mapreduce.JobContext
                                       context) throws IOException {
      super.commitJobInternal(context);
      if (firstTimeFail) {
        firstTimeFail = false;
        throw new IOException();
      } else {
        // succeed then, nothing to do
      }
    }
  }
}
