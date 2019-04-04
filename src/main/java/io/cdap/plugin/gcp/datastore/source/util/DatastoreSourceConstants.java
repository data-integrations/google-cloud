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
package io.cdap.plugin.gcp.datastore.source.util;

/**
 * Datastore source constants.
 */
public interface DatastoreSourceConstants {

  String PROPERTY_SERVICE_FILE_PATH = "serviceFilePath";
  String PROPERTY_PROJECT = "project";
  String PROPERTY_NAMESPACE = "namespace";
  String PROPERTY_KIND = "kind";
  String PROPERTY_ANCESTOR = "ancestor";
  String PROPERTY_FILTERS = "filters";
  String PROPERTY_NUM_SPLITS = "numSplits";
  String PROPERTY_KEY_TYPE = "keyType";
  String PROPERTY_KEY_ALIAS = "keyAlias";
  String PROPERTY_SCHEMA = "schema";

  String CONFIG_PROJECT = "mapred.gcd.input.project";
  String CONFIG_SERVICE_ACCOUNT_FILE_PATH = "mapred.gcd.input.service.account.file.path";
  String CONFIG_NAMESPACE = "mapred.gcd.input.namespace";
  String CONFIG_KIND = "mapred.gcd.input.kind";
  String CONFIG_QUERY = "mapred.gcd.input.query";
  String CONFIG_NUM_SPLITS = "mapred.gcd.input.num.splits";

}
