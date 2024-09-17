/*
 * Copyright © 2020 Cask Data, Inc.
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

import com.google.common.io.ByteStreams;
import com.google.crypto.tink.Aead;
import com.google.crypto.tink.JsonKeysetReader;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.KmsClients;
import com.google.crypto.tink.StreamingAead;
import com.google.crypto.tink.config.TinkConfig;
import com.google.crypto.tink.integration.gcpkms.GcpKmsClient;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.runtime.spi.runtimejob.ProgramRunFailureException;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.crypto.Decryptor;
import io.cdap.plugin.gcp.crypto.FSInputSeekableByteChannel;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link Decryptor} implementation for decrypting files encrypted using the Tink {@link StreamingAead} interface.
 */
public class TinkDecryptor implements Decryptor, Configurable {

  private static final String METADATA_SUFFIX = "io.cdap.crypto.metadata.suffix";
  private static final String KMS = "kms";
  private static final String KEYSET = "keyset";
  private static final String AAD = "aad";

  private Configuration configuration;
  private String metadataSuffix;

  public static Map<String, String> configure(String metadataSuffix, Map<String, String> properties) {
    properties.put(METADATA_SUFFIX, metadataSuffix);
    return properties;
  }

  public TinkDecryptor() throws GeneralSecurityException {
    TinkConfig.register();
  }

  @Override
  public SeekableByteChannel open(FileSystem fs, Path path, int bufferSize) throws IOException {
    DecryptInfo decryptInfo = getDecryptInfo(fs, path);
    Path metadataPath = new Path(path.getParent(), path.getName() + metadataSuffix);
    if (decryptInfo == null) {
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN),
          String.format("Missing encryption metadata for file '%s'. "
              + "Expected metadata path is '%s'.", path, metadataPath), null,
        ErrorType.USER, false, null);
    }

    try {
      StreamingAead streamingAead = decryptInfo.getKeysetHandle().getPrimitive(StreamingAead.class);
      return streamingAead.newSeekableDecryptingChannel(new FSInputSeekableByteChannel(fs, path, bufferSize),
                                                        decryptInfo.getAad());
    } catch (Exception e) {
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN),
          String.format("Unable to decrypt the file '%s' using encryption metadata file '%s'.",
              path, metadataPath), e.getMessage(), ErrorType.UNKNOWN, false, e);
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
    this.metadataSuffix = configuration.get(METADATA_SUFFIX);
    if (metadataSuffix == null) {
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN),
        String.format("Missing configuration '%s'.", METADATA_SUFFIX), null,
        ErrorType.USER, false, null);
    }
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  @Nullable
  private DecryptInfo getDecryptInfo(FileSystem fs, Path path) throws IOException {
    Path metadataPath = new Path(path.getParent(), path.getName() + metadataSuffix);

    if (!fs.exists(metadataPath)) {
      return null;
    }

    // Load the metadata
    JSONObject metadata;
    try (InputStream is = fs.open(metadataPath)) {
      metadata = new JSONObject(new String(ByteStreams.toByteArray(is), StandardCharsets.UTF_8));
    }

    // Create the DecryptInfo
    try {
      return getDecryptInfo(metadata);
    } catch (Exception e) {
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN),
          String.format("Unable to decrypt the file '%s' using encryption metadata file '%s'.",
              path, metadataPath), e.getMessage(), ErrorType.UNKNOWN, false, e);
    }
  }

  static DecryptInfo getDecryptInfo(JSONObject metadata) throws IOException {
    // Create the DecryptInfo
    try {
      String kmsURI = metadata.getString(KMS);
      KmsClients.add(new GcpKmsClient(kmsURI).withDefaultCredentials());
      Aead aead = KmsClients.get(kmsURI).getAead(kmsURI);
      KeysetHandle handle = KeysetHandle.read(JsonKeysetReader.withJsonObject(metadata.getJSONObject(KEYSET)), aead);
      byte[] aad = Base64.getDecoder().decode(metadata.getString(AAD));

      return new DecryptInfo(handle, aad);
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * A class that hold the {@link KeysetHandle} and the ADD for decryption.
   */
  private static final class DecryptInfo {
    private final KeysetHandle keysetHandle;
    private final byte[] aad;

    private DecryptInfo(KeysetHandle keysetHandle, byte[] aad) {
      this.keysetHandle = keysetHandle;
      this.aad = aad;
    }

    KeysetHandle getKeysetHandle() {
      return keysetHandle;
    }

    byte[] getAad() {
      return aad;
    }
  }
}
