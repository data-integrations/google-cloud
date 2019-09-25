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

package io.cdap.plugin.gcp.gcs;

import com.google.common.net.UrlEscapers;

import java.net.URI;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A path on GCS. Contains information about the bucket and blob name (if applicable).
 * A path is of the form gs://bucket/name.
 */
public class GCSPath {
  public static final String ROOT_DIR = "/";
  public static final String SCHEME = "gs://";
  private final URI uri;
  private final String bucket;
  private final String name;

  private GCSPath(URI uri, String bucket, String name) {
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
    if (path.isEmpty()) {
      throw new IllegalArgumentException("GCS path can not be empty. The path must be of form " +
                                           "'gs://<bucket-name>/path'.");
    }

    if (path.startsWith(ROOT_DIR)) {
      path = path.substring(1);
    } else if (path.startsWith(SCHEME)) {
      path = path.substring(SCHEME.length());
    }

    String bucket = path;
    int idx = path.indexOf(ROOT_DIR);
    // if the path within bucket is provided, then only get the bucket
    if (idx > 0) {
      bucket = path.substring(0, idx);
    }

    if (!Pattern.matches("[a-zA-Z0-9-_.]+", bucket)) {
      throw new IllegalArgumentException(String.format("Invalid bucket name in path '%s'. Bucket name should only " +
                                                         "contain alphanumeric, '-'. '_' and '.'.", path));
    }

    String file = idx > 0 ? path.substring(idx).replaceAll("^/", "") : "";
    URI uri = URI.create(SCHEME + bucket + "/" + UrlEscapers.urlFragmentEscaper().escape(file));
    return new GCSPath(uri, bucket, file);
  }
}
