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
package io.cdap.plugin.common;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

import java.util.Base64;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Validation Helper.
 */
public class ValidationHelper {
  private static String projectId = PluginPropertyUtils.pluginProp("projectId");
  private static final Gson gson = new Gson();

  private static final Logger LOG = LoggerFactory.getLogger(ValidationHelper.class);

  public static boolean validateBQDataToGCS(String table, String bucketName) throws IOException,
    InterruptedException {
    Map<String, JsonObject> bigQueryData = new HashMap<>();
    Map<String, JsonObject> gcsData = listBucketObjects(bucketName);

    getBigQueryTableData(table, bigQueryData);

    boolean isMatched = compareData(bigQueryData, gcsData);

    return isMatched;
  }

  public static boolean validateGCSDataToBQ(String bucketName, String table) throws IOException,
    InterruptedException {
    Map<String, JsonObject> bigQueryData = new HashMap<>();
    Map<String, JsonObject> gcsData = listBucketObjects(bucketName);

    getBigQueryTableData(table, bigQueryData);

    boolean isMatched = compareData(bigQueryData, gcsData);

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
        Map<String, JsonObject> dataMap2 = fetchObjectData(projectId, bucketName, objectName);
        bucketObjectData.putAll(dataMap2);
      }
    }

    return bucketObjectData;
  }

  private static Map<String, JsonObject> fetchObjectData(String projectId, String bucketName, String objectName) {
    Map<String, JsonObject> dataMap = new HashMap<>();
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    byte[] objectData = storage.readAllBytes(bucketName, objectName);
    String objectDataAsString = new String(objectData, StandardCharsets.UTF_8);

    // Splitting using the delimiter as a File can have more than one record.
    String[] lines = objectDataAsString.split("\n");
    for (String line : lines) {
      JsonObject json = gson.fromJson(line, JsonObject.class);
      String id = json.get("transaction_uid").getAsString();
      dataMap.put(id, json);
    }

    return dataMap;
  }

  private static void getBigQueryTableData(String table, Map<String, JsonObject> bigQueryRows)
    throws IOException, InterruptedException {

    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);
    for (FieldValueList row : result.iterateAll()) {
      JsonObject json = gson.fromJson(row.get(0).getStringValue(), JsonObject.class);
      JsonElement idElement = json.get("transaction_uid");
      if (idElement != null && idElement.isJsonPrimitive()) {
        String id = idElement.getAsString(); // Replace "id" with the actual key in the JSON
        bigQueryRows.put(id, json);
      } else {
        LOG.error("Data Mismatched");
      }
    }
  }

  private static boolean compareData(Map<String, JsonObject> map1, Map<String, JsonObject> map2) {
    if (!map1.keySet().equals(map2.keySet())) {
      return false;
    }
    for (String key : map1.keySet()) {
      JsonObject json1 = map1.get(key);
      JsonObject json2 = map2.get(key);
      if (!validateJsonElement(json1, json2)) {
        LOG.error("Data Mismatched");
      }
    }
    return true;
  }

  public static boolean validateJsonArray(JsonArray v1, JsonArray v2) {
    if (v1.size() != v2.size()) {
      return false;
    }
    for (int i = 0; i < v1.size(); i++) {
      if (!validateJsonElement(v1.get(i), v2.get(i))) {
        return false;
      }
    }
    return true;
  }

  public static boolean validateJsonElement(JsonElement v1, JsonElement v2) {
    if (v1.isJsonObject()) {
      if (!v2.isJsonObject()) {
        return false;
      }
      return validateJsonObject(v1.getAsJsonObject(), v2.getAsJsonObject());

    } else if (v1.isJsonArray()) {
      if (!v2.isJsonArray()) {
        return false;
      }
      return validateJsonArray(v1.getAsJsonArray(), v2.getAsJsonArray());

    } else if (v1.isJsonPrimitive()) {
      if (v2.isJsonPrimitive()) {
        return validateJsonPrimitive(v1.getAsJsonPrimitive(), v2.getAsJsonPrimitive());
      } else if (v2.isJsonArray()) {
        return validateByteArray(v1.getAsJsonPrimitive(), v2.getAsJsonArray());
      }
    } else if (v1.isJsonNull()) {
      return v2.isJsonNull();
    }

    return false;
  }

  public static boolean validateJsonObject(JsonObject v1, JsonObject v2) {
    for (Map.Entry<String, JsonElement> entry : v1.entrySet()) {
      String key = entry.getKey();
      JsonElement value1 = entry.getValue();
      JsonElement value2 = v2.get(key);

      if (!validateJsonElement(value1, value2)) {
        return false;
      }
    }
    return true;
  }

  public static boolean validateJsonPrimitive(JsonPrimitive v1, JsonPrimitive v2) {
    if (!v1.isJsonPrimitive() || !v2.isJsonPrimitive()) {
      return false;
    }

    JsonPrimitive p1 = v1.getAsJsonPrimitive();
    JsonPrimitive p2 = v2.getAsJsonPrimitive();

    if (p1.isString() && p2.isNumber()) {
      System.out.println("String number comparison" + p1 + p2);
      String s1 = p1.getAsString();
      long s2 = p2.getAsLong();

      try {
        LocalDate date = LocalDate.parse(s1);
        long epochDay = date.toEpochDay();
        if (epochDay == s2) {
          return true;
        }
      } catch (DateTimeParseException e3) {
        try {
          LocalTime time1 = LocalTime.parse(s1, DateTimeFormatter.ofPattern("HH:mm:ss"));
          long seconds1 = time1.truncatedTo(ChronoUnit.SECONDS).toSecondOfDay();
          LocalTime time = LocalTime.ofSecondOfDay(seconds1);
          String formattedTime = time.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
          if (formattedTime.equals(s1)) {
            return true;
          }
        } catch (DateTimeParseException e1) {
          // Date parsing failed, continue with time parsing

          try {
            LocalDateTime dateTime1 = LocalDateTime.parse(s1, DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));
            long epochSeconds1 = dateTime1.toEpochSecond(ZoneOffset.UTC) * 1_000_000L;

            if (epochSeconds1 == s2) {
              return true;
            }
          } catch (DateTimeParseException e) {
            // Date parsing failed, continue with other comparisons
          }

          return s1.equals(s2);
        }
      }
    }
    return p1.equals(p2);
  }

  public static boolean validateByteArray(JsonPrimitive v1, JsonArray v2) {
    if (!v1.isString()) {
      return false;
    }
    String s1 = v1.getAsString();
    byte[] bytes = new byte[v2.size()];
    for (int i = 0; i < v2.size(); i++) {
      bytes[i] = (byte) v2.get(i).getAsInt();
    }

    String s2 = Base64.getEncoder().encodeToString(bytes);
    return s1.equals(s2);
  }
}
