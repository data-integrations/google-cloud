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
package io.cdap.plugin.gcp.datastore.util;

import com.google.cloud.datastore.PathElement;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.datastore.v1.client.DatastoreHelper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Utility class that provides method for validation, parsing and conversion of config properties.
 */
public class DatastorePropertyUtil {

  private static final Pattern KEY_PATTERN = Pattern.compile("\\s*key\\s*\\((.*)\\)\\s*", Pattern.CASE_INSENSITIVE);
  public static final String DEFAULT_NAMESPACE = "";

  /**
   * Checks if given namespace value is not null or empty,
   * otherwise returns {@link DatastorePropertyUtil#DEFAULT_NAMESPACE}.
   *
   * @param namespace namespace value
   * @return valid namespace value
   */
  public static String getNamespace(@Nullable String namespace) {
    return Strings.isNullOrEmpty(namespace) ? DEFAULT_NAMESPACE : namespace;
  }

  /**
   * Parses given key literal string value into list of path elements
   * according to {@link DatastorePropertyUtil#KEY_PATTERN}.
   *
   * @param keyLiteral key literal
   * @return list of path elements
   */
  public static List<PathElement> parseKeyLiteral(String keyLiteral) {
    if (Strings.isNullOrEmpty(keyLiteral)) {
      return Collections.emptyList();
    }
    Matcher matcher = KEY_PATTERN.matcher(keyLiteral);
    if (!matcher.find()) {
      throw new IllegalArgumentException(
        String.format("Unsupported datastore key literal format: '%s'. Expected format: %s", keyLiteral,
                      "key(kind, identifier, kind, identifier, [...])"));
    }

    try {
      String keyString = matcher.group(1);
      String[] keyArray = keyString.split(",");
      Preconditions.checkState(keyArray.length % 2 == 0, "Key literal must have even number of elements");
      List<PathElement> resultList = new ArrayList<>(keyArray.length / 2);
      for (int i = 0; i < keyArray.length; i = i + 2) {
        String kind = keyArray[i].trim().replaceAll("`", "");
        String identifier = keyArray[i + 1].trim();
        PathElement pathElement = identifier.contains("'")
          ? PathElement.of(kind, identifier.replaceAll("'", ""))
          : PathElement.of(kind, Long.valueOf(identifier));
        resultList.add(pathElement);
      }
      return resultList;
    } catch (Exception e) {
      throw new IllegalArgumentException(
        String.format("Invalid Datastore key literal '%s'. Expected format: '%s'",
                      keyLiteral, "key(kind, identifier, kind, identifier, [...])"), e);
    }
  }

  /**
   * Checks given key alias value. If it is null or empty, returns {@link DatastoreHelper#KEY_PROPERTY_NAME}.
   *
   * @param keyAlias key alias value
   * @return valid key alias
   */
  public static String getKeyAlias(@Nullable String keyAlias) {
    return Strings.isNullOrEmpty(keyAlias) ? DatastoreHelper.KEY_PROPERTY_NAME : keyAlias;
  }

}
