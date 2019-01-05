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

package co.cask.gcp.gcs;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;

/**
 * A path on GCS. Contains information about the bucket and blob name (if applicable).
 * A path is of the form gs://bucket/name.
 */
public class GCSPath {
  public static final String ROOT_DIR = "/";
  private final URI uri;
  private final String bucket;
  private final String name;

  GCSPath(URI uri, String bucket, String name) {
    this.uri = uri;
    this.bucket = bucket;
    this.name = name;
  }

  public URI getUri() {
    return uri;
  }

  public String getBucket() {
    return bucket;
  }

  /**
   * @return the object name. This will be an empty string if the path represents a bucket.
   */
  public String getName() {
    return name;
  }

  boolean isBucket() {
    return name.isEmpty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GCSPath gcsPath = (GCSPath) o;
    return Objects.equals(uri, gcsPath.uri) &&
      Objects.equals(bucket, gcsPath.bucket) &&
      Objects.equals(name, gcsPath.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(uri, bucket, name);
  }

  /**
   * Parse the given path string into a GCSPath. Paths are expected to be of the form
   * gs://bucket/dir0/dir1/file, or bucket/dir0/dir1/file.
   *
   * @param path the path string to parse
   * @return the GCSPath for the given string.
   * @throws IllegalArgumentException if the path string is invalid
   */
  public static GCSPath from(String path) {
    URI uri = getURI(path);
    String bucket = uri.getAuthority();
    String name = uri.getPath();
    // strip preceding '/'. An empty name means it's for a bucket.
    name = name.isEmpty() ? name : name.substring(1);
    return new GCSPath(uri, bucket, name);
  }

  private static URI getURI(String path) {
    try {
      URI uri = new URI(path);
      if (uri.getScheme() != null && (!uri.getScheme().equalsIgnoreCase("gs") || uri.getAuthority() == null)) {
        throw new IllegalArgumentException(String.format("Invalid path '%s'. The path must be of form " +
                                                           "'gs://<bucket-name>/path'.", path));
      }
      if (uri.getScheme() == null) {
        return path.startsWith(ROOT_DIR) ? new URI("gs:/" + path) : new URI("gs://" + path);
      }
      return uri;
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(String.format("Invalid path '%s'. The path must be of form " +
                                                         "'gs://<bucket-name>/path'.", path), e);
    }
  }
}
