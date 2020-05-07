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

package io.cdap.plugin.gcp.firestore.sink.util;

/**
 * Firestore Sink constants.
 */
public interface FirestoreSinkConstants {

  /**
   * Configuration property name used to specify the type of document id.
   */
  String PROPERTY_ID_TYPE = "documentIdType";

  /**
   * Configuration property name used to specify column name to be used as document id.
   */
  String PROPERTY_ID_ALIAS = "idAlias";

  /**
   * Configuration property name used to specify the batch size.
   */
  String PROPERTY_BATCH_SIZE = "batchSize";

  /**
   * Maximum number of entities that can be passed to a Commit operation in the Cloud Firestore API
   */
  int MAX_BATCH_SIZE = 500;
}
