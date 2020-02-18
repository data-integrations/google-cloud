/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.gcp.firestore.source;

import com.google.cloud.firestore.QueryDocumentSnapshot;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Firestore input format
 */
public class FirestoreInputFormat extends InputFormat<Object, QueryDocumentSnapshot> {
  //private static final Logger LOG = LoggerFactory.getLogger(FirestoreInputFormat.class);

  @Override
  public List<InputSplit> getSplits(JobContext jobContext) throws IOException, InterruptedException {
    return Collections.singletonList(new FirestoreInputSplit());
    /*
    Configuration conf = jobContext.getConfiguration();

    long maxSplitSize = conf.getLong(PROPERTY_NUM_SPLITS, 1);
    if (maxSplitSize == 1) {
      return Collections.singletonList(new FirestoreInputSplit());
    }

    int count = getRecordCount(conf);
    long chunkSize = (long) count / maxSplitSize;
    List<InputSplit> splits = new ArrayList<>();

    for (int i = 0; i < maxSplitSize; ++i) {
      InputSplit split;
      if (i + 1 == maxSplitSize) {
        split = new FirestoreInputSplit((long) i * chunkSize, count);
      } else {
        split = new FirestoreInputSplit((long) i * chunkSize, (long) i * chunkSize + chunkSize);
      }

      splits.add(split);
    }

    return splits;
    */
  }

  @Override
  public RecordReader<Object, QueryDocumentSnapshot> createRecordReader(InputSplit inputSplit,
                                                                        TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {
    return new FirestoreRecordReader();
  }

  /*
  private int getRecordCount(Configuration conf) {
    String projectId = conf.get(NAME_PROJECT);
    String databaseId = conf.get(PROPERTY_DATABASE_ID);
    String serviceAccountFilePath = conf.get(NAME_SERVICE_ACCOUNT_FILE_PATH);
    String collection = conf.get(PROPERTY_COLLECTION);
    int count = 0;

    try {
      Firestore db = FirestoreUtil.getFirestore(serviceAccountFilePath, projectId, databaseId);
      ApiFuture<QuerySnapshot> query = db.collection(collection).get();
      QuerySnapshot querySnapshot = query.get();
      count = querySnapshot.getDocuments().size();
      db.close();

    } catch (Exception e) {
      LOG.error("Error", e);
      count = 1;
    }
    return count;
  }
  */
}
