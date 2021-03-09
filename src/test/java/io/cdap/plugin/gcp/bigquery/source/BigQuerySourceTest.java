package io.cdap.plugin.gcp.bigquery.source;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import io.cdap.plugin.gcp.bigquery.util.BigQueryConstants;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Test;

import java.util.UUID;

public class BigQuerySourceTest {

  @Test
  public void test() throws Exception {

    String serviceAccount = "/Users/yaojie/Downloads/yjcdaptest-14bfc9d30518.json";

    UUID uuid = UUID.randomUUID();
    Configuration configuration = BigQueryUtil.getBigQueryConfig(serviceAccount, "yjcdaptest", null,
                                                                 "filePath");

    String bucket = uuid.toString();

    configuration.set("fs.default.name", String.format("gs://%s/%s/", bucket, uuid));
    configuration.setBoolean("fs.gs.impl.disable.cache", true);
    configuration.setBoolean("fs.gs.metadata.cache.enable", false);

    configuration.set(BigQueryConstants.CONFIG_SERVICE_ACCOUNT, "/Users/yaojie/Downloads/yjcdaptest-14bfc9d30518.json");
    configuration.setBoolean(BigQueryConstants.CONFIG_SERVICE_ACCOUNT_IS_FILE, true);

    String temporaryGcsPath = String.format("gs://%s/%s/hadoop/input/%s", bucket, uuid, uuid);
    PartitionedBigQueryInputFormat.setTemporaryCloudStorageDirectory(configuration, temporaryGcsPath);
    BigQueryConfiguration.configureBigQueryInput(configuration, "yjcdaptest",
                                                 "persons_info", "testtable");

    Job job = Job.getInstance(configuration);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);

    job.setJobID(new JobID("test", 0));
    TaskID taskId = new TaskID(job.getJobID(), TaskType.MAP, 0);
    TaskAttemptContext taskContext = new TaskAttemptContextImpl(configuration, new TaskAttemptID(taskId, 0));

    PartitionedBigQueryInputFormat inputFormat = new PartitionedBigQueryInputFormat();
    InputSplit split = inputFormat.getSplits(job).get(0);
    try (RecordReader<LongWritable, GenericData.Record> reader = inputFormat.createRecordReader(split, taskContext)) {
      reader.initialize(split, taskContext);
      while (reader.nextKeyValue()) {
        String record = reader.getCurrentValue().toString();
      }
    }
  }
}
