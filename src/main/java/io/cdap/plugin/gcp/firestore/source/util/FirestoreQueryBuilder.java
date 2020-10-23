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

package io.cdap.plugin.gcp.firestore.source.util;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import io.cdap.plugin.gcp.firestore.source.FirestoreInputSplit;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.IOException;
import java.util.List;

/**
 * The class that provides helper method to build the Firestore {@link Query} object.
 */
public class FirestoreQueryBuilder {
  /**
   * Returns the Query.
   * @param db the database id
   * @param collection the collection likes a table
   * @param fields the fields of collection
   * @param inputSplit the inputSplit is a limit
   * @param filters the filter for given field as well as value
   * @return The instance of {@link Query} object
   * @throws IOException on issues with file reading
   * @throws InterruptedException on issues, interrupting when reading a file
   */
  public static Query buildQuery(Firestore db, String collection, List<String> fields, InputSplit inputSplit,
                                 List<FilterInfo> filters) throws IOException, InterruptedException {
    int splitStart = 0;
    int splitLength = 0;

    if (inputSplit instanceof FirestoreInputSplit) {
      FirestoreInputSplit split = (FirestoreInputSplit) inputSplit;
      splitStart = (int) split.getStart();
      splitLength = (int) split.getLength();
    }

    Query query = db.collection(collection);

    if (!fields.isEmpty()) {
      query = query.select(fields.toArray(new String[0]));
    }

    if (!filters.isEmpty()) {
      for (FilterInfo filter : filters) {
        switch (filter.getOperator()) {
          case EQUAL_TO:
          case NUMERIC_EQUAL_TO:
            query = query.whereEqualTo(filter.getField(), filter.getValue());
            break;
          case LESS_THAN:
            query = query.whereLessThan(filter.getField(), filter.getValue());
            break;
          case LESS_THAN_OR_EQUAL_TO:
            query = query.whereLessThanOrEqualTo(filter.getField(), filter.getValue());
            break;
          case GREATER_THAN:
            query = query.whereGreaterThan(filter.getField(), filter.getValue());
            break;
          case GREATER_THAN_OR_EQUAL_TO:
            query = query.whereGreaterThanOrEqualTo(filter.getField(), filter.getValue());
            break;
        }
      }
    }

    if (splitLength > 0) {
      query = query.offset(splitStart).limit(splitLength);
    }

    return query;
  }
}
