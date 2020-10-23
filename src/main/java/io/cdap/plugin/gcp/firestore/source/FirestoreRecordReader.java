/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.common.base.Splitter;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.firestore.source.util.FilterInfo;
import io.cdap.plugin.gcp.firestore.source.util.FilterInfoParser;
import io.cdap.plugin.gcp.firestore.source.util.FirestoreQueryBuilder;
import io.cdap.plugin.gcp.firestore.source.util.FirestoreSourceConstants;
import io.cdap.plugin.gcp.firestore.util.FirestoreConstants;
import io.cdap.plugin.gcp.firestore.util.FirestoreUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * {@link FirestoreRecordReader} reads the data from Firestore.
 */
public class FirestoreRecordReader extends RecordReader<Object, QueryDocumentSnapshot> {
  private static final Logger LOG = LoggerFactory.getLogger(FirestoreRecordReader.class);
  private Configuration conf;
  private Firestore db;
  private List<QueryDocumentSnapshot> items;
  // Map key that represents the item index.
  private LongWritable key;
  // Map value that represents an item.
  private QueryDocumentSnapshot value;
  private Iterator<QueryDocumentSnapshot> iterator;
  private long itemIdx;

  @Override
  public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
    throws IOException, InterruptedException {

    conf = taskAttemptContext.getConfiguration();
    String projectId = conf.get(GCPConfig.NAME_PROJECT);
    String databaseId = conf.get(FirestoreConstants.PROPERTY_DATABASE_ID);
    String serviceAccountFilePath = conf.get(GCPConfig.NAME_SERVICE_ACCOUNT_FILE_PATH);
    String collection = conf.get(FirestoreConstants.PROPERTY_COLLECTION);
    List<String> fields = Splitter.on(',').trimResults()
      .splitToList(conf.get(FirestoreSourceConstants.PROPERTY_SCHEMA, ""));
    List<String> pullDocuments = Splitter.on(',').trimResults()
      .splitToList(conf.get(FirestoreSourceConstants.PROPERTY_PULL_DOCUMENTS, ""));
    List<String> skipDocuments = Splitter.on(',').trimResults()
      .splitToList(conf.get(FirestoreSourceConstants.PROPERTY_SKIP_DOCUMENTS, ""));
    String customQuery = conf.get(FirestoreSourceConstants.PROPERTY_CUSTOM_QUERY, "");

    db = FirestoreUtil.getFirestore(serviceAccountFilePath, projectId, databaseId);

    try {
      List<FilterInfo> filters = getParsedFilters(customQuery);
      Query query = FirestoreQueryBuilder.buildQuery(db, collection, fields, inputSplit, filters);
      ApiFuture<QuerySnapshot> futureSnapshot = query.get();

      QuerySnapshot querySnapshot = futureSnapshot.get();
      List<QueryDocumentSnapshot> documents = querySnapshot.getDocuments();

      if (!pullDocuments.isEmpty()) {
        documents = documents.stream().filter(o -> pullDocuments.contains(o.getId())).collect(Collectors.toList());
      }

      if (!skipDocuments.isEmpty()) {
        documents = documents.stream().filter(o -> !skipDocuments.contains(o.getId())).collect(Collectors.toList());
      }

      items = documents;

      LOG.debug("documents={}", items.size());

      iterator = items.iterator();
      itemIdx = 0;
    } catch (ExecutionException e) {
      LOG.error("Error in Reader", e);
      throw new InterruptedException(e.getMessage());
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (iterator == null || !iterator.hasNext()) {
      return false;
    }
    QueryDocumentSnapshot item = iterator.next();
    key = new LongWritable(itemIdx);
    itemIdx++;
    value = item;
    return true;
  }

  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return key;
  }

  @Override
  public QueryDocumentSnapshot getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void close() throws IOException {
  }

  private List<FilterInfo> getParsedFilters(String filterString) {
    List<FilterInfo> filters = Collections.emptyList();
    try {
      filters = FilterInfoParser.parseFilterString(filterString);
    } catch (Exception e) {
      LOG.warn("Failed while parsing the filter string", e);
    }
    return filters;
  }
}
