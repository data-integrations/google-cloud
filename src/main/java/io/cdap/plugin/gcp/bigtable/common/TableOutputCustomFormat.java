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

package io.cdap.plugin.gcp.bigtable.common;

import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.mapreduce.JobContext;

import java.io.IOException;

/**
 * Table output format class with null credentials fix fon non-default service accounts
 */
public class TableOutputCustomFormat extends TableOutputFormat {
  public TableOutputCustomFormat() {

  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    setConf(context.getConfiguration());
    super.checkOutputSpecs(context);
  }
}
