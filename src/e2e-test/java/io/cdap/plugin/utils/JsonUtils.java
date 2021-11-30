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
package io.cdap.plugin.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * JsonUtils contains the helper functions.
 */
public class JsonUtils {
  private static final Logger logger = Logger.getLogger(JsonUtils.class);

  public static List<KeyValue> covertJsonStringToKeyValueList(String json) {
    ObjectMapper mapper = new ObjectMapper();
    List<KeyValue> keyValueList = new ArrayList<KeyValue>();
    {
      try {
        keyValueList = Arrays.asList(mapper.readValue(json, KeyValue[].class));
      } catch (IOException e) {
        logger.error("Error while converting JsonString to keyValueList: " + e);
      }
    }
    return keyValueList;
  }
}

