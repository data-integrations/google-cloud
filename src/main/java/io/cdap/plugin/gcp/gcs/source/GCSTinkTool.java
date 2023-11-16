/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.gcp.gcs.source;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.KeysetWriter;
import com.google.crypto.tink.KmsClients;
import com.google.crypto.tink.StreamingAead;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import com.google.crypto.tink.streamingaead.StreamingAeadConfig;
import com.google.crypto.tink.streamingaead.StreamingAeadKeyTemplates;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * This tool is adapted from the
 * <a href="https://cloud.google.com/kms/docs/client-side-encryption#java">KMS example</a>.
 * This can be used to upload files to GCS to test the decryption logic in the GCS source.
 * Follow https://cloud.google.com/kms/docs/create-key for instructions on creating a KMS key.
 *
 * <p>
 * A command-line utility for encrypting small files with envelope encryption and uploading the
 * results to GCS.
 *
 * <p>The CLI takes the following required arguments:
 *
 * <ul>
 *   <li>kek-uri: The URI for the Cloud KMS key to be used for envelope encryption.
 *       Should be of the form gcp-kms://projects/[project]/locations/[loc]/keyRings/[keyring]/cryptoKeys/[key].
 *   <li>gcp-project-id: The ID of the GCP project hosting the GCS blobs that you want to encrypt or
 *       decrypt.
 *   <li>local-input-file: Read the plaintext from this local file.
 *   <li>gcs-output-blob: Write the encryption result to this blob in GCS. A corresponding .metadata file will also
 *       be written, containing the information the {@link TinkDecryptor} expects.
 * </ul>
 */
public final class GCSTinkTool {
  private static final String GCS_PATH_PREFIX = "gs://";

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.printf("Expected 4 parameters, got %d\n", args.length);
      System.err.println(
        "Usage: java GcsEnvelopeAeadExample kek-uri gcp-project-id input-file output-file");
      System.exit(1);
    }
    String kekUri = args[0];
    String gcpProjectId = args[1];

    // Initialise Tink: register all Streaming AEAD key types with the Tink runtime
    StreamingAeadConfig.register();

    // Read the GCP credentials and set up client
    try {
      KmsClients.add(new GcpKmsClient(kekUri).withDefaultCredentials());
    } catch (GeneralSecurityException ex) {
      System.err.println("Error initializing GCP client: " + ex);
      System.exit(1);
    }

    // Create envelope AEAD primitive using AES256 GCM for encrypting the data
    KeysetHandle handle = KeysetHandle.generateNew(StreamingAeadKeyTemplates.AES256_GCM_HKDF_4KB);
    StreamingAead aead = handle.getPrimitive(StreamingAead.class);

    Storage storage =
      StorageOptions.newBuilder()
        .setProjectId(gcpProjectId)
        .build()
        .getService();

    // Encrypt the local file
    byte[] input = Files.readAllBytes(Paths.get(args[2]));
    String gcsBlobPath = args[3];
    // This will bind the encryption to the location of the GCS blob. That if, if you rename or
    // move the blob to a different bucket, decryption will fail.
    // See https://developers.google.com/tink/aead#associated_data.
    byte[] associatedData = gcsBlobPath.getBytes(UTF_8);
    ByteArrayOutputStream ciphertextOutputStream = new ByteArrayOutputStream();
    try (OutputStream encryptingOutputStream = aead.newEncryptingStream(ciphertextOutputStream, associatedData)) {
      encryptingOutputStream.write(input);
    }
    byte[] ciphertext = ciphertextOutputStream.toByteArray();
    // Upload encrypted file to GCS
    writeToGcs(storage, gcsBlobPath, ciphertext);
    // Upload metadata file to GCS
    String metadataBlobPath = gcsBlobPath + ".metadata";
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    KeysetWriter keySetWriter = JsonKeysetWriter.withOutputStream(bos);
    Aead keysetAead = KmsClients.get(kekUri).getAead(kekUri);
    handle.write(keySetWriter, keysetAead);
    JSONObject keySetObj = new JSONObject(new String(bos.toByteArray(), UTF_8));

    JSONObject metadataObj = new JSONObject();
    metadataObj.put("kms", kekUri);
    metadataObj.put("aad", Base64.getEncoder().encodeToString(associatedData));
    metadataObj.put("keyset", keySetObj);
    // verify it is valid
    TinkDecryptor.getDecryptInfo(metadataObj);

    writeToGcs(storage, metadataBlobPath, metadataObj.toString(4).getBytes(UTF_8));

    System.exit(0);
  }

  private static void writeToGcs(Storage storage, String path, byte[] content) {
    String bucketName = getBucketName(path);
    String objectName = getObjectName(path);
    BlobId blobId = BlobId.of(bucketName, objectName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
    storage.create(blobInfo, content);
  }

  private static String getBucketName(String gcsBlobPath) {
    if (!gcsBlobPath.startsWith(GCS_PATH_PREFIX)) {
      throw new IllegalArgumentException(
        "GCS blob paths must start with gs://, got " + gcsBlobPath);
    }

    String bucketAndObjectName = gcsBlobPath.substring(GCS_PATH_PREFIX.length());
    int firstSlash = bucketAndObjectName.indexOf("/");
    if (firstSlash == -1) {
      throw new IllegalArgumentException(
        "GCS blob paths must have format gs://my-bucket-name/my-object-name, got " + gcsBlobPath);
    }
    return bucketAndObjectName.substring(0, firstSlash);
  }

  private static String getObjectName(String gcsBlobPath) {
    if (!gcsBlobPath.startsWith(GCS_PATH_PREFIX)) {
      throw new IllegalArgumentException(
        "GCS blob paths must start with gs://, got " + gcsBlobPath);
    }

    String bucketAndObjectName = gcsBlobPath.substring(GCS_PATH_PREFIX.length());
    int firstSlash = bucketAndObjectName.indexOf("/");
    if (firstSlash == -1) {
      throw new IllegalArgumentException(
        "GCS blob paths must have format gs://my-bucket-name/my-object-name, got " + gcsBlobPath);
    }
    return bucketAndObjectName.substring(firstSlash + 1);
  }
}
