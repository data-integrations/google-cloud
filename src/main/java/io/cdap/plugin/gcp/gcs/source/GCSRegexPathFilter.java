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

package io.cdap.plugin.gcp.gcs.source;

import io.cdap.plugin.format.RegexPathFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.util.Map;
import java.util.regex.Pattern;

/**
 * A {@link PathFilter} for filtering out excluded file pattern.
 */
public class GCSRegexPathFilter extends RegexPathFilter {

  private static final String EXCLUSION_REGEX = "path.filter.exclude.regex";

  private Pattern exclusionPattern;

  public static Map<String, String> configure(GCSSource.GCSSourceConfig sourceConfig, Map<String, String> properties) {
    Pattern pattern = sourceConfig.getExclusionPattern();
    if (pattern != null) {
      properties.put(EXCLUSION_REGEX, pattern.pattern());
      properties.put(FileInputFormat.PATHFILTER_CLASS, GCSRegexPathFilter.class.getName());
    }
    return properties;
  }

  @Override
  public boolean accept(Path path) {
    if (!super.accept(path)) {
      return false;
    }
    return exclusionPattern == null || !exclusionPattern.matcher(path.toUri().getPath()).matches();
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf == null) {
      return;
    }
    String pattern = conf.getRaw(EXCLUSION_REGEX);
    if (pattern != null) {
      exclusionPattern = Pattern.compile(pattern);
    }
  }
}
