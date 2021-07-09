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

import javax.annotation.Nullable;

/**
 * Contains constant keys for externalized strings, to be used as reference for
 * internationalization/localization of messages and text. The keys when passed
 * to any method of {@link io.cdap.plugin.gcp.dataplex.sink.util.ResourceText}, bring corresponding text message in a
 * language based on the specified or default locale from the properties
 * files (i10n).
 *
 */
public enum ResourceConstants {

 // ERR_MISSING_PARAM_PREFIX(null, "err.missing.param.prefix")
  ;

  private final String code;
  private final String key;

  ResourceConstants(@Nullable String code, String key) {
    this.code = code;
    this.key = key;
  }

  @Nullable
  public String getCode() {
    return code;
  }

  public String getKey() {
    return key;
  }

  public String getMsgForKeyWithCode() {
    return getMsgForKey(code);
  }

  public String getMsgForKeyWithCode(Object... params) {
    Object[] destArr = new Object[params.length + 1];
    destArr[0] = code;
    System.arraycopy(params, 0, destArr, 1, params.length);

    return getMsgForKey(destArr);
  }

  public String getMsgForKey() {
    return io.cdap.plugin.gcp.dataplex.sink.util.ResourceText.getString(key);
  }

  public String getMsgForKey(Object... params) {
    return io.cdap.plugin.gcp.dataplex.sink.util.ResourceText.getString(key, params);
  }
}
