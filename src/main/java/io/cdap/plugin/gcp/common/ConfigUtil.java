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

package io.cdap.plugin.gcp.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Utilities for plugin configuration
 */
public class ConfigUtil {
  public static Map<String, String> parseKeyValueConfig(String configValue, String delimiter,
                                                        String keyValueDelimiter) {
    Map<String, String> map = new HashMap<>();
    for (String property : configValue.split(delimiter)) {
      String[] parts = property.split(keyValueDelimiter);
      String key = parts[0];
      String value = parts[1];
      map.put(key, value);
    }
    return map;
  }

  public static String getKVPair(String key, String value, String keyValueDelimiter) {
    return String.format("%s%s%s", key, keyValueDelimiter, value);
  }
}
