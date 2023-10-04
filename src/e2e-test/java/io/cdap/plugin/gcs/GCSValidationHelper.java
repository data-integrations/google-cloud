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

import au.com.bytecode.opencsv.CSVReader;
import com.esotericsoftware.minlog.Log;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.utils.DataFileFormat;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
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
  private static final String csvFilePath = PluginPropertyUtils.pluginProp("gcsCsvExpectedFilePath");
  private static final String jsonFilePath = PluginPropertyUtils.pluginProp("gcsMultipleFilesRegexFilePath");
  private static final Gson gson = new Gson();
  private static final Logger LOG = LoggerFactory.getLogger(GCSValidationHelper.class);

  /**
   * Validates data in a Google Cloud Storage (GCS) bucket against expected JSON content.
   *
   * @param bucketName The name of the GCS bucket to validate.
   * @return true if the GCS bucket's content matches the expected JSON data, false otherwise.
   */
  public static boolean validateGCSSourceToGCSSinkWithJsonFormat(String bucketName) {
    Map<String, JsonObject> expectedTextJsonData = new HashMap<>();
    getFileData(jsonFilePath, expectedTextJsonData);
    Map<String, JsonObject> actualGcsCsvData = listBucketObjects(bucketName, DataFileFormat.JSON);
    boolean isMatched = actualGcsCsvData.equals(expectedTextJsonData);
    return isMatched;
  }

  /**
   * Validates if the data in a (GCS) bucket matches the expected CSV data in JSON format.
   *
   * @param bucketName The name of the GCS bucket to validate.
   * @return True if the GCS CSV data matches the expected data, false otherwise.
   * @throws IOException If an IO error occurs during data retrieval.
   */
  public static boolean validateGCSSourceToGCSSinkWithCSVFormat(String bucketName) {
    Map<String, JsonObject> expectedCSVData = readCsvFileDataAndConvertToJson(csvFilePath);
    Map<String, JsonObject> actualGcsCsvData = listBucketObjects(bucketName, DataFileFormat.CSV);

    boolean isMatched = actualGcsCsvData.equals(expectedCSVData);

    return isMatched;
  }

  /**
   * Validates if the data in a (GCS) bucket matches the data
   * obtained from Avro files converted to JSON.
   *
   * @param bucketName The name of the GCS bucket to validate.
   * @return True if the GCS data matches the Avro data, false otherwise.
   * @throws IOException If an IO error occurs during data retrieval.
   */
  public static boolean validateGCSSourceToGCSSinkWithAVROFormat(String bucketName) throws IOException {
    Map<String, JsonObject> expectedAvroData = convertAvroToJsonWithKeys(avroFilePath);
    Map<String, JsonObject> actualGcsData = listBucketObjects(bucketName, DataFileFormat.JSON);

    boolean isMatched = actualGcsData.equals(expectedAvroData);

    return isMatched;
  }


  public static Map<String, JsonObject> listBucketObjects(String bucketName, DataFileFormat dataFormat) {
    Map<String, JsonObject> bucketObjectData = new HashMap<>();
    String projectId = PluginPropertyUtils.pluginProp("projectId");
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
        Map<String, JsonObject> dataMap = fetchObjectData(projectId, bucketName, objectName, dataFormat);
        bucketObjectData.putAll(dataMap);
      }
    }

    return bucketObjectData;
  }

  /**
   * Fetches and parses data from a specified object in a GCS bucket.
   *
   * @param projectId  The ID of the GCP project where the GCS bucket is located.
   * @param bucketName The name of the GCS bucket containing the object to fetch.
   * @param objectName The name of the object to fetch from the GCS bucket.
   * @param format     The format of the object data (JSON or CSV).
   * @return A Map containing the parsed data from the object, with string keys and JSON objects as values.
   */
  public static Map<String, JsonObject> fetchObjectData(String projectId, String bucketName, String objectName,
                                                        DataFileFormat format) {
    Map<String, JsonObject> dataMap = new HashMap<>();
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    byte[] objectData = storage.readAllBytes(bucketName, objectName);
    String objectDataAsString = new String(objectData, StandardCharsets.UTF_8);

    switch (format) {
      case JSON:
        parseDataToJson(objectDataAsString, dataMap);
        break;
      case CSV:
        parseCsvDataToJson(objectDataAsString, dataMap);
        break;
      default:
        LOG.error("Unsupported File Format");
        break;
    }
    return dataMap;
  }

  private static void parseDataToJson(String data, Map<String, JsonObject> dataMap) {
    String[] lines = data.split("\n");
    for (String line : lines) {
      JsonObject json = gson.fromJson(line, JsonObject.class);
      String id = json.get("id").getAsString();
      dataMap.put(id, json);
    }
  }

  private static void parseCsvDataToJson(String data, Map<String, JsonObject> dataMap) {
    String[] lines = data.split("\n");
    String[] headers = lines[0].split(",");

    for (int lineCount = 1; lineCount < lines.length; lineCount++) {
      String[] values = lines[lineCount].split(",");
      JsonObject jsonObject = new JsonObject();
      for (int headerCount = 0; headerCount < headers.length; headerCount++) {
        jsonObject.addProperty(headers[headerCount], values[headerCount]);
      }
      String id = values[0];
      dataMap.put(id, jsonObject);
    }
  }

  /**
   * Converts data from a CSV filePath to a map of JSON objects.
   *
   * @return A map with identifiers (e.g., ID from the first column) as keys and JSON objects as values.
   * @throws IOException If there's an error reading the CSV file.
   */
  public static Map<String, JsonObject> readCsvFileDataAndConvertToJson(String filePath) {
    Map<String, JsonObject> csvDataMap = new HashMap<>();
    try (CSVReader csvReader = new CSVReader(new java.io.FileReader(filePath))) {
      // Read the header line to get column names
      String[] headers = csvReader.readNext();

      String[] line;
      while ((line = csvReader.readNext()) != null) {
        JsonObject jsonObject = new JsonObject();

        for (int j = 0; j < headers.length; j++) {
          jsonObject.addProperty(headers[j], line[j]);
        }
        String id = line[0];
        csvDataMap.put(id, jsonObject);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return csvDataMap;
  }

  /**
   * Converts Avro filePath to JSON objects with keys and stores them in a map.
   *
   * @return A map of keys to JSON objects representing the Avro data.
   * @throws IOException If an IO error occurs during Avro to JSON conversion.
   */
  public static Map<String, JsonObject> convertAvroToJsonWithKeys(String filePath) throws IOException {
    File avroFile = new File(filePath);
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

  /**
   * Reads data from a JSON file, parses each line into JSON objects, and populates a provided
   * map with these objects, using the "ID" field as the key.
   *
   * @param fileName The name of the JSON file to read.
   * @param fileMap  A map where parsed JSON objects will be stored with their "ID" field as the key.
   */
  public static void getFileData(String fileName, Map<String, JsonObject> fileMap) {
    try (BufferedReader br = new BufferedReader(new java.io.FileReader(fileName))) {
      String line;
      while ((line = br.readLine()) != null) {
        JsonObject json = gson.fromJson(line, JsonObject.class);
        if (json.has("id")) { // Check if the JSON object has the "id" key
          JsonElement idElement = json.get("id");
          if (idElement.isJsonPrimitive()) {
            String idKey = idElement.getAsString();
            fileMap.put(idKey, json);
          } else {
            Log.error("ID key not found");
          }
        }
      }
    } catch (IOException e) {
      System.err.println("Error reading the file: " + e.getMessage());
    }
  }
}
