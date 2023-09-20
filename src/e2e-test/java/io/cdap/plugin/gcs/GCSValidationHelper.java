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

package io.cdap.plugin.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * GCS Validation Helper.
 */
public class GCSValidationHelper {
  private static final String avroFilePath = PluginPropertyUtils.pluginProp("gcsAvroExpectedFilePath");
  private static final String projectId = PluginPropertyUtils.pluginProp("projectId");
  private static final Gson gson = new Gson();

  /**
   * Validates if the data in a (GCS) bucket matches the data
   * obtained from Avro files converted to JSON.
   *
   * @param bucketName The name of the GCS bucket to validate.
   * @return True if the GCS data matches the Avro data, false otherwise.
   * @throws IOException If an IO error occurs during data retrieval.
   */
  public static boolean validateGCSSourceToGCSSink(String bucketName) throws IOException {
    Map<String, JsonObject> expectedAvroData = convertAvroToJsonWithKeys();
    Map<String, JsonObject> actualGcsData = listBucketObjects(bucketName);
    boolean isMatched = actualGcsData.equals(expectedAvroData);
    return isMatched;
  }

  public static Map<String, JsonObject> listBucketObjects(String bucketName) {
    Map<String, JsonObject> bucketObjectData = new HashMap<>();
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    Page<Blob> blobs = storage.list(bucketName);

    List<Blob> bucketObjects = StreamSupport.stream(blobs.iterateAll().spliterator(), true)
      .filter(blob -> blob.getSize() != 0)
      .collect(Collectors.toList());

    Stream<String> objectNamesWithData = bucketObjects.stream().map(blob -> blob.getName());
    List<String> bucketObjectNames = objectNamesWithData.collect(Collectors.toList());

    if (!bucketObjectNames.isEmpty()) {
      String objectName = bucketObjectNames.get(0);
      if (objectName.contains("part-r")) {
        Map<String, JsonObject> dataMap = fetchObjectData(projectId, bucketName, objectName);
        bucketObjectData.putAll(dataMap);
      }
    }
    return bucketObjectData;
  }

  /**
   * Fetches the data of a specific object from a GCS bucket
   * and converts it to a map of JSON objects.
   *
   * @param projectId  The ID of the GCP project.
   * @param bucketName The name of the GCS bucket containing the object.
   * @param objectName The name of the object to fetch.
   * @return A map of object data where keys are IDs and values are JSON objects.
   */
  private static Map<String, JsonObject> fetchObjectData(String projectId, String bucketName, String objectName) {
    Map<String, JsonObject> dataMap = new HashMap<>();
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    byte[] objectData = storage.readAllBytes(bucketName, objectName);
    String objectDataAsString = new String(objectData, StandardCharsets.UTF_8);

    // Splitting using the delimiter as a File can have more than one record.
    String[] lines = objectDataAsString.split("\n");
    for (String line : lines) {
      JsonObject json = gson.fromJson(line, JsonObject.class);
      String id = json.get("id").getAsString();
      dataMap.put(id, json);
    }
    return dataMap;
  }

  /**
   * Converts Avro files to JSON objects with keys and stores them in a map.
   *
   * @return A map of keys to JSON objects representing the Avro data.
   * @throws IOException If an IO error occurs during Avro to JSON conversion.
   */
  public static Map<String, JsonObject> convertAvroToJsonWithKeys() throws IOException {
    File avroFile = new File(avroFilePath);
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    Map<String, JsonObject> avroDataMap = new HashMap<>();

    try (FileReader<GenericRecord> dataFileReader = DataFileReader.openReader(avroFile, datumReader)) {
      int keyCounter = 1;
      while (dataFileReader.hasNext()) {
        GenericRecord record = dataFileReader.next();
        String json = record.toString();
        JsonObject jsonObject = gson.fromJson(json, JsonObject.class);
        avroDataMap.put(String.valueOf(keyCounter), jsonObject);
        keyCounter++;
      }
    }
    return avroDataMap;
  }
}
