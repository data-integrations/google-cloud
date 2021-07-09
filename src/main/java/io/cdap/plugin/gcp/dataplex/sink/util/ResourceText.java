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

package io.cdap.plugin.gcp.dataplex.sink.util;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

import javax.annotation.Nullable;

/**
 * Util class to load and use the localized resource bundles for text and
 * messages.
 *
 */
public class ResourceText {

  private static final String CONNECTOR_BUNDLE = "properties/i10n/DataplexBatchSinkBundle";

  private ResourceText() {
  }

  public static ResourceBundle getBundle() {
    return ResourceBundle.getBundle(CONNECTOR_BUNDLE);
  }

  public static ResourceBundle getBundle(Locale locale) {
    return ResourceBundle.getBundle(CONNECTOR_BUNDLE, locale);
  }

  public static String getString(String key) {
    if (getBundle() == null) {
      return key;
    }

    return getBundle().getString(key);
  }

  public static String getString(Locale locale, String key) {
    if (getBundle(locale) == null) {
      return getString(key);
    }

    return getBundle(locale).getString(key);
  }

  public static String getString(String key, @Nullable Object... params) {
    String pattern = getString(key);
    return MessageFormat.format(pattern, params);
  }

  public static String getString(Locale locale, String key, Object... params) {
    String pattern = getString(locale, key);
    return MessageFormat.format(pattern, params);
  }
}
