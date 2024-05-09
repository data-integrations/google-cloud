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
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Output Committer which creates and delegates operations to other GCS Output Committer instances.
 * <p>
 * Delegated instances are created based on a supplied Output Format and Destination Table Names.
 */
public class DelegatingGCSOutputCommitter extends OutputCommitter {

  private TaskAttemptContext taskAttemptContext;
  private boolean firstTable = true;
  private static final String PARTITIONS_FILE_SUFFIX = "_partitions.txt";

  public DelegatingGCSOutputCommitter(TaskAttemptContext taskAttemptContext) {
    this.taskAttemptContext = taskAttemptContext;
  }

  /**
   * Add a new GCSOutputCommitter based on a supplied Output Format and Table Name.
   * <p>
   * This GCS Output Committer gets initialized when created.
   */
  @SuppressWarnings("rawtypes")
  public void addGCSOutputCommitterFromOutputFormat(OutputFormat outputFormat,
                                                    String tableName) throws IOException, InterruptedException {
    //Set output directory
    taskAttemptContext.getConfiguration().set(FileOutputFormat.OUTDIR,
                                              DelegatingGCSOutputUtils.buildOutputPath(
                                                taskAttemptContext.getConfiguration(), tableName));

    //Wrap output committer into the GCS Output Committer.
    GCSOutputCommitter gcsOutputCommitter = new GCSOutputCommitter(outputFormat.getOutputCommitter(taskAttemptContext));

    gcsOutputCommitter.setupJob(taskAttemptContext);
    gcsOutputCommitter.setupTask(taskAttemptContext);
    writePartitionFile(taskAttemptContext.getConfiguration().get(FileOutputFormat.OUTDIR), taskAttemptContext);
    firstTable = false;
  }

  @Override
  public void setupJob(JobContext jobContext) throws IOException {
    Path outputPath = new Path(jobContext.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    FileSystem fs = outputPath.getFileSystem(jobContext.getConfiguration());
    Path tempPath = new Path(outputPath, getPendingDirPath(jobContext.getJobID()));
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
      FileOutputCommitter committer = new FileOutputCommitter(new Path(output), taskAttemptContext);
      committer.commitTask(taskAttemptContext);
    }
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    for (String output : getOutputPaths(jobContext)) {
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
    fs.delete(new Path(outputPath, getPendingDirPath(jobContext.getJobID())), true);
  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
    IOException ioe = null;
    for (String output : getOutputPaths(taskAttemptContext)) {
      try {
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
    try {
      for (String output : getOutputPaths(jobContext)) {
        taskAttemptContext.getConfiguration().set(FileOutputFormat.OUTDIR, output);
        FileOutputCommitter committer = new FileOutputCommitter(new Path(output), taskAttemptContext);
        committer.abortJob(jobContext, state);
      }
    } catch (IOException e) {
      if (ioe == null) {
        ioe = e;
      } else {
        ioe.addSuppressed(e);
      }
    } finally {
      cleanupJob(jobContext);
    }
    if (ioe != null) {
      throw ioe;
    }
  }

  // return path lists based on JobContext configuration.
  private Set<String> getOutputPaths(JobContext jobContext) throws IOException {
    Path outputPath = new Path(jobContext.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    FileSystem fs = outputPath.getFileSystem(jobContext.getConfiguration());
    return getOutputPathsFromTempPartitionFile(outputPath, fs, null, jobContext.getJobID());
  }

  private Set<String> getOutputPaths(TaskAttemptContext taskAttemptContext) throws IOException {
    Path outputPath = new Path(
      taskAttemptContext.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    FileSystem fs = outputPath.getFileSystem(taskAttemptContext.getConfiguration());
    return getOutputPathsFromTempPartitionFile(outputPath, fs,
                                               taskAttemptContext.getTaskAttemptID().getTaskID().toString(),
                                               taskAttemptContext.getJobID());
  }

  /**
   * This method will return the full path up to path suffix after reading from partitions.txt file
   * If method is getting called from task context, it will return paths from single file, otherwise all paths
   *
   * @param baseOutputPath
   * @param fs
   * @param taskId
   * @param jobID
   * @return
   * @throws IOException
   */
  private Set<String> getOutputPathsFromTempPartitionFile(Path baseOutputPath, FileSystem fs, @Nullable String taskId,
                                                          JobID jobID) throws IOException {
    Set<String> outputPaths = new HashSet<>();
    Path tempPath = taskId == null ? new Path(baseOutputPath, getPendingDirPath(jobID))
      : new Path(baseOutputPath, String.format("%s/%s%s", getPendingDirPath(jobID), taskId,
                                               PARTITIONS_FILE_SUFFIX));

    if (!fs.exists(tempPath)) {
      return outputPaths;
    }

    for (FileStatus status : fs.listStatus(tempPath)) {
      if (status.getPath().getName().endsWith(PARTITIONS_FILE_SUFFIX)) {
        try (FSDataInputStream dis = fs.open(status.getPath())) {
          while (true) {
            try {
              outputPaths.add(dis.readUTF());
            } catch (EOFException e) {
              break;
            }
          }
        }
      }
    }
    return outputPaths;
  }

  /**
   * This method will create a _temporary_{jobID} directory in base directory path and will create a file with name
   * {taskid}_partitions.txt which will store the full path upto path suffix. e.g. gs://basepath/tablename/path_suffix
   *
   * @param path    Split file path upto split field name
   * @param context
   * @throws IOException
   */
  private void writePartitionFile(String path, TaskAttemptContext context) throws IOException {
    Path outputPath = new Path(context.getConfiguration().get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR));
    Path tempPath = new Path(outputPath, getPendingDirPath(context.getJobID()));
    FileSystem fs = tempPath.getFileSystem(context.getConfiguration());
    String taskId = context.getTaskAttemptID().getTaskID().toString();
    Path taskPartitionFile = new Path(tempPath, String.format("%s%s", taskId, PARTITIONS_FILE_SUFFIX));
    if (!fs.exists(taskPartitionFile)) {
      fs.createNewFile(taskPartitionFile);
    } else if (firstTable) {
      fs.create(taskPartitionFile, true);
    }
    try (DataOutputStream out = fs.append(taskPartitionFile)) {
      out.writeUTF(path);
    }
  }

  // This will create a directory with name _temporary_{jobId} to write the partition files
  // Job ID added as a suffix, so that multiple pipelines can write to same path in parallel.
  private String getPendingDirPath(JobID jobId) {
    return String.format("%s_%s", FileOutputCommitter.PENDING_DIR_NAME, jobId);
  }

}
