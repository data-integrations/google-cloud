/*
 * Copyright © 2026 Cask Data, Inc.
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
import com.google.crypto.tink.JsonKeysetWriter;
import com.google.crypto.tink.KeysetHandle;
import com.google.crypto.tink.StreamingAead;
import com.google.crypto.tink.aead.AeadConfig;
import com.google.crypto.tink.aead.AeadKeyTemplates;
import com.google.crypto.tink.streamingaead.StreamingAeadConfig;
import com.google.crypto.tink.streamingaead.StreamingAeadKeyTemplates;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Base64;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Checks that the encryption metadata written by {@link GCSTinkTool} can be parsed with org.json and read back by
 * Tink the same way {@link TinkDecryptor} does. A local AEAD key stands in for the Cloud KMS key.
 */
public class TinkKeysetJsonTest {

  private static final int ITERATIONS = 10;

  @BeforeClass
  public static void setupClass() throws Exception {
    AeadConfig.register();
    StreamingAeadConfig.register();
  }

  @Test
  public void testMetadataRoundTrip() throws Exception {
    Aead keysetAead = KeysetHandle.generateNew(AeadKeyTemplates.AES128_GCM).getPrimitive(Aead.class);
    byte[] plaintext = "some plaintext to encrypt".getBytes(UTF_8);
    byte[] associatedData = "gs://bucket/path/file.csv".getBytes(UTF_8);

    // Each iteration uses a new keyset, so different random key ids go through the JSON round trip.
    for (int i = 0; i < ITERATIONS; i++) {
      KeysetHandle handle = KeysetHandle.generateNew(StreamingAeadKeyTemplates.AES256_GCM_HKDF_4KB);

      // Encrypt and write the metadata the same way as GCSTinkTool.
      ByteArrayOutputStream ciphertext = new ByteArrayOutputStream();
      try (OutputStream os = handle.getPrimitive(StreamingAead.class).newEncryptingStream(ciphertext,
                                                                                          associatedData)) {
        os.write(plaintext);
      }
      ByteArrayOutputStream keyset = new ByteArrayOutputStream();
      handle.write(JsonKeysetWriter.withOutputStream(keyset), keysetAead);
      JSONObject metadataObj = new JSONObject();
      metadataObj.put("kms", "gcp-kms://projects/p/locations/global/keyRings/r/cryptoKeys/k");
      metadataObj.put("aad", Base64.getEncoder().encodeToString(associatedData));
      metadataObj.put("keyset", new JSONObject(new String(keyset.toByteArray(), UTF_8)));
      String metadataJson = metadataObj.toString(4);

      // Parse and read it back the same way as TinkDecryptor.
      JSONObject metadata = new JSONObject(metadataJson);
      KeysetHandle readHandle = KeysetHandle.read(JsonKeysetReader.withJsonObject(metadata.getJSONObject("keyset")),
                                                  keysetAead);
      byte[] aad = Base64.getDecoder().decode(metadata.getString("aad"));
      Assert.assertEquals(handle.getKeysetInfo().getPrimaryKeyId(), readHandle.getKeysetInfo().getPrimaryKeyId());

      try (InputStream is = readHandle.getPrimitive(StreamingAead.class)
        .newDecryptingStream(new ByteArrayInputStream(ciphertext.toByteArray()), aad)) {
        Assert.assertArrayEquals(plaintext, ByteStreams.toByteArray(is));
      }
    }
  }
}
