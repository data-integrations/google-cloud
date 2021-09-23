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

package io.cdap.plugin.gcp.gcs;

import io.cdap.plugin.format.FileFormat;

/**
 * Utility class for GCS formats.
 */
public class Formats {

  private Formats() {
    // no-op
  }

  /**
   * Get the plugin name for the given format. Older versions of the source and sink allowed case insensitive
   * format names. For example, 'Avro' and 'avro' were treated the same.
   *
   * After formats were moved to plugins, the name is now case sensitive. So for any of the pre-packaged formats,
   * this method will lowercase the given format name. If it is a custom format, it is returned as-is.
   *
   * @param formatName the raw format name
   * @return the name of the format plugin
   */
  public static String getFormatPluginName(String formatName) {
    // need to do this for backwards compatibility, where the pre-packaged format names were case insensitive.
    try {
      FileFormat fileFormat = FileFormat.from(formatName, x -> true);
      return fileFormat.name().toLowerCase();
    } catch (IllegalArgumentException e) {
      // ignore
    }
    return formatName;
  }
}
