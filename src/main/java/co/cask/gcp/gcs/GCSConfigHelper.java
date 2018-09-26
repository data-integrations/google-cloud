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
package co.cask.gcp.gcs;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * A helper class for GCS configs
 */
public final class GCSConfigHelper {

  private static final String FILE_SYSTEM_PATH_PREFIX = "gs://";

  /**
   * Returns the given path as it is if it starts with {@link GCSConfigHelper#FILE_SYSTEM_PATH_PREFIX} else appends it
   * in the beginning.
   *
   * @param path the path
   * @return {@link URI} for the given path
   * @throws IllegalArgumentException if the given path is not valid
   */
  public static URI getPath(String path) {
    try {
      URI uri = new URI(path);
      if (uri.getScheme() != null && (!uri.getScheme().equalsIgnoreCase("gs") || uri.getAuthority() == null)) {
        throw new IllegalArgumentException(String.format("Invalid path '%s'. The path must be of form " +
                                                           "'gs://<bucket-name>/path'.", path));
      }
      if (uri.getScheme() == null) {
        return new URI(FILE_SYSTEM_PATH_PREFIX + uri.getSchemeSpecificPart());
      }
      return uri;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("Invalid path '%s'. The path must be of form " +
                                                         "'gs://<bucket-name>/path'.", path), e);
    }
  }

  /**
   * Returns the bucket name from the given path
   *
   * @param path the path from which bucket name needs to be extracted
   * @return the bucket name
   */
  public static String getBucket(String path) {
    return getPath(path).getAuthority();
  }
}
