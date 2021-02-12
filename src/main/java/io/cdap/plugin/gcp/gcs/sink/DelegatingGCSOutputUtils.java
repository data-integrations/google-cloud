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

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.io.IOException;

/**
 * Utility class for the Delegating GCS Output classes.
 */
public class DelegatingGCSOutputUtils {

  @SuppressWarnings("unchecked")
  public static OutputFormat<NullWritable, StructuredRecord> getDelegateFormat(Configuration hConf) throws IOException {
    String delegateClassName = hConf.get(DelegatingGCSOutputFormat.DELEGATE_CLASS);
    try {
      Class<OutputFormat<NullWritable, StructuredRecord>> delegateClass =
        (Class<OutputFormat<NullWritable, StructuredRecord>>) hConf.getClassByName(delegateClassName);
      return delegateClass.newInstance();
    } catch (Exception e) {
      throw new IOException("Unable to instantiate output format for class " + delegateClassName, e);
    }
  }

  public static String buildOutputPath(Configuration hConf, String context) {
    return String.format("%s/%s/%s",
                         hConf.get(DelegatingGCSOutputFormat.OUTPUT_PATH_BASE_DIR),
                         context,
                         hConf.get(DelegatingGCSOutputFormat.OUTPUT_PATH_SUFFIX));
  }
}
