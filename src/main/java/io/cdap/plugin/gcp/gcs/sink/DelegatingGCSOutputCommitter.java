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

package io.cdap.plugin.gcp.gcs.sink;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Output Committer which creates and delegates operations to other GCS Output Committer instances.
 * <p>
 * Delegated instances are created based on a supplied Output Format and Destination Table Names.
 */
public class DelegatingGCSOutputCommitter extends OutputCommitter {

  private TaskAttemptContext taskAttemptContext;
  private static final String PARTITIONS_FILE_SUFFIX = "_partitions.txt";


  public DelegatingGCSOutputCommitter() {
  }

  // Setting Task Context to committer to use at the time of commit job to get task details
  public void setTaskContext(TaskAttemptContext taskContext) {
    taskAttemptContext = taskContext;
  }

  /**
   * Add a new GCSOutputCommitter based on a supplied Output Format and Table Name.
   * <p>
   * This GCS Output Committer gets initialized when created.
   */
  @SuppressWarnings("rawtypes")
  public void addGCSOutputCommitterFromOutputFormat(OutputFormat outputFormat,
                                                    TaskAttemptContext context,
                                                    String tableName) throws IOException, InterruptedException {
    //Set output directory
    context.getConfiguration().set(FileOutputFormat.OUTDIR,
                                   DelegatingGCSOutputUtils.buildOutputPath(context.getConfiguration(), tableName));

    //Wrap output committer into the GCS Output Committer.
    GCSOutputCommitter gcsOutputCommitter = new GCSOutputCommitter(outputFormat.getOutputCommitter(context));

    gcsOutputCommitter.setupJob(context);
    gcsOutputCommitter.setupTask(context);
    writePartitionFile(context.getConfiguration().get(FileOutputFormat.OUTDIR), context);
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    Path outputPath = new Path(jobContext.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    FileSystem fs = outputPath.getFileSystem(jobContext.getConfiguration());
    Path tempPath = new Path(outputPath, FileOutputCommitter.PENDING_DIR_NAME);
    // creating the _temporary folder in base path
    fs.mkdirs(tempPath);
  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {
    //no-op
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    return true;
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {
    for (String output : getOutputPaths(taskAttemptContext)) {
      taskAttemptContext.getConfiguration().set(FileOutputFormat.OUTDIR, output);
      FileOutputCommitter committer = new FileOutputCommitter(new Path(output), taskAttemptContext);
      committer.commitTask(taskAttemptContext);
    }
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    for (String output : getOutputPaths(jobContext)) {
      jobContext.getConfiguration().set(FileOutputFormat.OUTDIR, output);
      // todo this taskAttemptContext is coming from Driver class with mapper context and is same for all output paths,
      //  even though it is working fine still needs to check how it works
      FileOutputCommitter committer = new FileOutputCommitter(new Path(output), taskAttemptContext);
      committer.commitJob(jobContext);
    }
    cleanupJob(jobContext);
  }

  @Override
  public void cleanupJob(JobContext jobContext) throws IOException {
    Path outputPath = new Path(jobContext.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    FileSystem fs = outputPath.getFileSystem(jobContext.getConfiguration());
    // delete the temporary directory that has partition information in text files.
    fs.delete(new Path(outputPath, FileOutputCommitter.PENDING_DIR_NAME), true);
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    IOException ioe = null;
    for (String output : getOutputPaths(taskAttemptContext)) {
      try {
        taskAttemptContext.getConfiguration().set(FileOutputFormat.OUTDIR, output);
        FileOutputCommitter committer = new FileOutputCommitter(new Path(output), taskAttemptContext);
        committer.abortTask(taskAttemptContext);
      } catch (IOException e) {
        if (ioe == null) {
          ioe = e;
        } else {
          ioe.addSuppressed(e);
        }
      }
    }

    if (ioe != null) {
      throw ioe;
    }
  }

  @Override
  public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
    IOException ioe = null;

    for (String output : getOutputPaths(jobContext)) {
      try {
        taskAttemptContext.getConfiguration().set(FileOutputFormat.OUTDIR, output);
        FileOutputCommitter committer = new FileOutputCommitter(new Path(output), taskAttemptContext);
        committer.abortJob(jobContext, state);
      } catch (IOException e) {
        if (ioe == null) {
          ioe = e;
        } else {
          ioe.addSuppressed(e);
        }
      }
    }
    cleanupJob(jobContext);
    if (ioe != null) {
      throw ioe;
    }
  }

  // return path lists based on JobContext configuration.
  private Set<String> getOutputPaths(JobContext jobContext) throws IOException {
    Path outputPath = new Path(jobContext.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    FileSystem fs = outputPath.getFileSystem(jobContext.getConfiguration());
    return getOutputPathsFromTempPartitionFile(outputPath, fs);
  }

  // return path lists based on TaskAttemptContext configuration.
  private Set<String> getOutputPaths(TaskAttemptContext context) throws IOException {
    Path outputPath = new Path(context.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    FileSystem fs = outputPath.getFileSystem(context.getConfiguration());
    return getOutputPathsFromTempPartitionFile(outputPath, fs);
  }

  /**
   * This method will return the full path up to path suffix after reading from partitions.txt file
   *
   * @param baseOutputPath
   * @param fs
   * @return
   * @throws IOException
   */
  private Set<String> getOutputPathsFromTempPartitionFile(Path baseOutputPath, FileSystem fs) throws IOException {
    Path tempPath = new Path(baseOutputPath, FileOutputCommitter.PENDING_DIR_NAME);
    Set<String> outputPaths = new HashSet<>();

    for (FileStatus status : fs.listStatus(tempPath)) {
      if (status.getPath().getName().endsWith(PARTITIONS_FILE_SUFFIX)) {
        try (FSDataInputStream dis = fs.open(status.getPath());
             DataInputStream in = new DataInputStream(new BufferedInputStream(dis));
             BufferedReader br = new BufferedReader(new java.io.InputStreamReader(in))) {
          String line;
          while ((line = br.readLine()) != null) {
            outputPaths.add(line);
          }
        }
      }
    }
    return outputPaths;
  }

  /**
   * This method will create a _temporary directory in base directory path and will create a file with name
   * {taskid}_partitions.txt which will store the full path upto path suffix. e.g. gs://basepath/tablename/path_suffix
   *
   * @param path
   * @param context
   * @throws IOException
   */
  private void writePartitionFile(String path, TaskAttemptContext context) throws IOException {
    Path outputPath = new Path(context.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    Path tempPath = new Path(outputPath, FileOutputCommitter.PENDING_DIR_NAME);
    FileSystem fs = tempPath.getFileSystem(context.getConfiguration());
    String taskId = context.getTaskAttemptID().getTaskID().toString();
    Path taskPartitionFile = new Path(tempPath, String.format("%s%s", taskId, PARTITIONS_FILE_SUFFIX));
    if (!fs.exists(taskPartitionFile)) {
      fs.createNewFile(taskPartitionFile);
    }
    try (DataOutputStream out = fs.append(taskPartitionFile)) {
      out.writeBytes(path + "\n");
    }
    taskAttemptContext = context;
  }

}
