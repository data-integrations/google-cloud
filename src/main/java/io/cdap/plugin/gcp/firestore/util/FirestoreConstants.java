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

package io.cdap.plugin.gcp.firestore.util;

/**
 * Firestore constants.
 */
public interface FirestoreConstants {

  /**
   * Firestore plugin name.
   */
  String PLUGIN_NAME = "Firestore";

  /**
   * Configuration property name used to specify Firestore database name.
   */
  String PROPERTY_DATABASE_ID = "databaseId";

  /**
   * Configuration property name used to specify name of the database collection.
   */
  String PROPERTY_COLLECTION = "collection";

  /**
   * Default column name to be used when document ids to be included in output schema.
   */
  String ID_PROPERTY_NAME = "__id__";
}
