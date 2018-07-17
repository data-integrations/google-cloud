/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.spanner;

/**
 * Collection of constants used by <code>SpannerOutputFormat</code>.
 */
public final class SpannerConstants {
  public static final String PROJECT_ID = "project.id";
  public static final String INSTANCE_ID = "instance.id";
  public static final String DATABASE = "database.name";
  public static final String TABLE_NAME = "table.name";
  public static final String SERVICE_ACCOUNT_FILE_PATH = "service.account.path";
}
