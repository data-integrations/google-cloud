/*
 * Copyright © 2019-2020 Cask Data, Inc.
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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.common.annotations.VisibleForTesting;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * A wrapper around the GCS storage client that has extended logic around recursively copying a "directory" on GCS.
 */
public class StorageClient {
  private static final Logger LOG = LoggerFactory.getLogger(StorageClient.class);
  private final Storage storage;

  private StorageClient(Storage storage) {
    this.storage = storage;
  }

  /**
   * Picks one blob that has the path prefix and is not ending with '/'
   * @param path
   * @return
   */
  public Blob pickABlob(String path) {
    if (path == null || path.isEmpty()) {
      return null;
    }
    GCSPath gcsPath = GCSPath.from(path);
    Page<Blob> blobPage = storage.list(gcsPath.getBucket(), Storage.BlobListOption.prefix(gcsPath.getName()));
    Iterator<Blob> iterator = blobPage.getValues().iterator();
    while (iterator.hasNext()) {
      Blob blob = iterator.next();
      if (blob.getName().endsWith("/")) {
        continue;
      }
      return blob;
    }
    return null;
  }

  /**
   * Updates the metadata for the blob
   * @param blob
   * @param metaData
   */
  public void setMetaData(Blob blob, Map<String, String> metaData) {
    if (blob == null || metaData == null || metaData.isEmpty()) {
      return;
    }
    storage.update(BlobInfo.newBuilder(blob.getBlobId()).setMetadata(metaData).build());
  }

  /**
   * Applies the given function with metadata of each blobs in the path
   * @param path
   * @param function
   */
  public void mapMetaDataForAllBlobs(String path, Consumer<Map<String, String>> function) {
    if (path == null || path.isEmpty() || function == null) {
      return;
    }
    GCSPath gcsPath = GCSPath.from(path);
    Page<Blob> blobPage = storage.list(gcsPath.getBucket(), Storage.BlobListOption.prefix(gcsPath.getName()));
    Iterator<Blob> blobIterator = blobPage.iterateAll().iterator();
    while (blobIterator.hasNext()) {
      Blob blob = blobIterator.next();
      Map<String, String> metadata = blob.getMetadata();
      if (metadata == null) {
        continue;
      }
      function.accept(metadata);
    }
  }

  /**
   * Copy objects from the source path to the destination path. If the source path is a single object, that object
   * will be copied to the destination. If the source path represents a directory, objects within the directory
   * will be copied to the destination directory.
   *
   * @param sourcePath the path to copy objects from
   * @param destPath the path to copy objects to
   * @param recursive whether to copy objects in all subdirectories
   * @param overwrite whether to overwrite existing objects
   * @throws IllegalArgumentException if overwrite is false and copying would overwrite an existing object
   */
  public void copy(GCSPath sourcePath, GCSPath destPath, boolean recursive, boolean overwrite) {
    pairTraverse(sourcePath, destPath, recursive, overwrite, BlobPair::copy);
  }

  /**
   * Move objects from the source path to the destination path. If the source path is a single object, that object
   * will be moved to the destination. If the source path represents a directory, objects within the directory
   * will be moved to the destination directory.
   *
   * @param sourcePath the path to move objects from
   * @param destPath the path to move objects to
   * @param recursive whether to move objects in all subdirectories
   * @param overwrite whether to overwrite existing objects
   * @throws IllegalArgumentException if overwrite is false and moving would overwrite an existing object
   */
  public void move(GCSPath sourcePath, GCSPath destPath, boolean recursive, boolean overwrite) {
    pairTraverse(sourcePath, destPath, recursive, overwrite, BlobPair::move);
  }

  /**
   * Gets source and destination pairs by traversing the source path. Consumes each pair after the directory structure
   * is completely traversed.
   */
  private void pairTraverse(GCSPath sourcePath, GCSPath destPath, boolean recursive, boolean overwrite,
                            Consumer<BlobPair> consumer) {

    Bucket sourceBucket = null;
    try {
      sourceBucket = storage.get(sourcePath.getBucket());
    } catch (StorageException e) {
      // Add more descriptive error message
      throw new RuntimeException(
        String.format("Unable to access source bucket %s. ", sourcePath.getBucket())
          + "Ensure you entered the correct bucket path.", e);
    }
    if (sourceBucket == null) {
      throw new IllegalArgumentException(
        String.format("Source bucket '%s' does not exist.", sourcePath.getBucket()));
    }
    Bucket destBucket = null;
    try {
      destBucket = storage.get(destPath.getBucket());
    } catch (StorageException e) {
      // Add more descriptive error message
      throw new RuntimeException(
        String.format("Unable to access destination bucket %s. ", destPath.getBucket())
          + "Ensure you entered the correct bucket path.", e);
    }
    if (destBucket == null) {
      throw new IllegalArgumentException(
        String.format("Destination bucket '%s' does not exist. Please create it first.", destPath.getBucket()));
    }

    boolean destinationBaseExists;
    String baseDestName = destPath.getName();
    if (destPath.isBucket() || storage.get(BlobId.of(destPath.getBucket(), baseDestName)) != null) {
      destinationBaseExists = true;
    } else {
      // if gs://bucket2/subdir doesn't exist, check if gs://bucket2/subdir/ exists
      // similarly, if gs://bucket2/subdir/ doesn't exist, check if gs://bucket2/subdir exists
      // this is because "cp dir0 subdir" and "cp dir0 subdir/" are equivalent if the 'subdir' directory exists
      String modifiedName = baseDestName.endsWith("/") ?
        baseDestName.substring(0, baseDestName.length() - 1) : baseDestName + "/";
      destinationBaseExists = storage.get(BlobId.of(destPath.getBucket(), modifiedName)) != null;
    }

    List<BlobPair> copyList = new ArrayList<>();
    traverse(BlobId.of(sourcePath.getBucket(), sourcePath.getName()), recursive, sourceBlob -> {
      BlobId destBlobID = resolve(sourcePath.getName(), sourceBlob.getBlobId().getName(),
                                  destPath, destinationBaseExists);
      if (!overwrite) {
        Blob destBlob = storage.get(destBlobID);
        // we can't just use Blob's isDirectory() because the cloud console will create a 'directory' by creating
        // a 0 size placeholder blob that ends with '/'. This placeholder blob's isDirectory() method returns false,
        // but we don't want the overwrite check to fail on it. So we explicitly ignore the check for these 0 size
        // placeholder blobs.
        if (destBlob != null && !destBlob.getName().endsWith("/") && destBlob.getSize() != 0) {
          throw new IllegalArgumentException(String.format("%s already exists.", toPath(destBlobID)));
        }
      }
      copyList.add(new BlobPair(sourceBlob, destBlobID));
    });

    LOG.debug("Found {} objects.", copyList.size());
    for (BlobPair blobPair : copyList) {
      consumer.accept(blobPair);
    }
  }

  /**
   * Resolves what the destination blob id should be when copying/moving the source blob.
   *
   * Suppose gs://bucket0/dir1/dir2 is being recursively copied to gs://bucket1/subdir and the following object exists:
   *
   * gs://bucket0/dir1/dir2/a/b/c
   *
   * In this example, baseName = dir1/dir2, sourceName = dir1/dir2/a/b/c, and dest = gs://bucket1/subdir.
   *
   * If gs://bucket1/subdir already exists, 'dir2' should be copied into the 'subdir' directory,
   * resolving to gs://bucket1/subdir/dir2/a/b/c.
   * If gs://bucket1/subdir does not already exist, 'dir2' should become the 'subdir' directory,
   * resolving to gs://bucket1/subdir/a/b/c.
   *
   * @param baseName the base object that is being copied or moved
   * @param sourceName the actual object that is being copied or moved
   * @param dest the object destination
   * @param destExists whether the destination exists
   * @return the full destination
   */
  @VisibleForTesting
  static BlobId resolve(String baseName, String sourceName, GCSPath dest, boolean destExists) {
    // the relative part is the part of the sourceName that comes after the baseName.
    // if baseName = dir1/dir2/ and sourceName = dir1/dir2/a/b/c, the relative part is /a/b/c
    String relativePart = sourceName.substring(baseName.length());

    if (dest.isBucket()) {
      // if the destination is a bucket, just use the source name with that bucket
      return BlobId.of(dest.getBucket(), sourceName);
    }

    // if the destination exists or ends in '/', take the last part of the baseName and append that to the destination,
    // ex: subdir -> subdir/dir2
    // after that, append the relative part
    // ex: subdir/dir2 -> subdir/dir2/a/b/c
    // also do this if the destination ends with '/'.
    if (destExists || dest.getName().endsWith("/")) {
      int lastDirIndex = baseName.lastIndexOf("/");
      String lastPart = lastDirIndex > 0 ? baseName.substring(lastDirIndex) : baseName;
      return BlobId.of(dest.getBucket(), append(append(dest.getName(), lastPart), relativePart));
    }

    // if the destination doesn't exist and doesn't end in '/', append the relative part to the destination
    return BlobId.of(dest.getBucket(), append(dest.getName(), relativePart));
  }

  // appends a part to a base, making sure there is one '/' separating them, assuming the base does not end with more
  // than one '/' and part does not start with more than one '/'.
  @VisibleForTesting
  static String append(String base, String part) {
    boolean baseEndsWithDivider = base.endsWith("/");
    boolean partStartWithDivider = part.startsWith("/");
    if (baseEndsWithDivider && partStartWithDivider) {
      return base.substring(0, base.length() - 1) + part;
    } else if (!baseEndsWithDivider && !base.isEmpty() && !partStartWithDivider && !part.isEmpty()) {
      return base + "/" + part;
    } else {
      return base + part;
    }
  }

  /**
   * Add all objects (non-directory blobs) that exist for the given blob id. If the id is an object itself, that blob
   * is added to the collection. If it represents a directory, all objects within that directory are added.
   * If recursive is true, all subdirectories will also be searched.
   * If the blob does not exist and does not represent a directory, nothing happens.
   *
   * @param blobId the blob id to traverse
   * @param recursive whether to recursively traverse subdirectories
   * @param consumer the blob consumer
   */
  private void traverse(BlobId blobId, boolean recursive, Consumer<Blob> consumer) {
    Page<Blob> blobList = storage.list(blobId.getBucket(), Storage.BlobListOption.currentDirectory(),
                                       Storage.BlobListOption.prefix(blobId.getName()));
    for (Blob blob : blobList.iterateAll()) {
      if (!blob.isDirectory()) {
        consumer.accept(blob);
      } else if (recursive) {
        traverse(blob.getBlobId(), true, consumer);
      }
    }
  }

  private static String toPath(BlobId blobId) {
    return String.format("gs://%s/%s", blobId.getBucket(), blobId.getName());
  }

  public static StorageClient create(String project, @Nullable String serviceAccountPath) throws IOException {
    StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(project);
    if (serviceAccountPath != null) {
      builder.setCredentials(GCPUtils.loadServiceAccountCredentials(serviceAccountPath));
    }
    Storage storage = builder.build().getService();
    return new StorageClient(storage);
  }

  /**
   * Represents a blob to be copied or moved.
   */
  private static class BlobPair {
    private final Blob sourceBlob;
    private final BlobId destination;

    private BlobPair(Blob sourceBlob, BlobId destination) {
      this.sourceBlob = sourceBlob;
      this.destination = destination;
    }

    private Blob copy() {
      LOG.debug("Copying {} to {}.", toPath(sourceBlob.getBlobId()), toPath(destination));
      CopyWriter copyWriter = sourceBlob.copyTo(destination);
      Blob copied = copyWriter.getResult();
      LOG.debug("Successfully copied {} to {}.", toPath(sourceBlob.getBlobId()), toPath(destination));
      return copied;
    }

    private Blob move() {
      Blob moved = copy();
      LOG.debug("Deleting {}.", toPath(sourceBlob.getBlobId()));
      sourceBlob.delete();
      LOG.debug("Successfully deleted {}.", toPath(sourceBlob.getBlobId()));
      return moved;
    }

  }
}
