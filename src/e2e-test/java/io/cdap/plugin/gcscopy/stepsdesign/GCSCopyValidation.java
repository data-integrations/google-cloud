package io.cdap.plugin.gcscopy.stepsdesign;

import au.com.bytecode.opencsv.CSVReader;
import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.PluginPropertyUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * GCSCopy plugin Validation.
 */
public class GCSCopyValidation {

  private static final String projectId = PluginPropertyUtils.pluginProp("projectId");
  public static boolean validateGCSSourceToGCSSinkWithCSVFormat(String bucketName, String filepath) {
    Map<String, JsonObject> expectedCSVData = readCsvAndConvertToJson(filepath);
    Map<String, JsonObject> actualGcsCsvData = listBucketObjects(bucketName);

    boolean isMatched = actualGcsCsvData.equals(expectedCSVData);

    return isMatched;
  }

  public static Map<String, JsonObject> readCsvAndConvertToJson(String filepath) {
    Map<String, JsonObject> csvDataMap = new HashMap<>();
    try (CSVReader csvReader = new CSVReader(new java.io.FileReader(filepath))) {
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

  public static Map<String, JsonObject> fetchObjectData(String projectId, String bucketName, String objectName) {
    Map<String, JsonObject> dataMap = new HashMap<>();
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    byte[] objectData = storage.readAllBytes(bucketName, objectName);
    String objectDataAsString = new String(objectData, StandardCharsets.UTF_8);
    String[] lines = objectDataAsString.split("\n");
    String[] headers = lines[0].split(",");

    for (int i = 1; i < lines.length; i++) {
      String[] values = lines[i].split(",");
      JsonObject jsonObject = new JsonObject();
      for (int j = 0; j < headers.length; j++) {
        jsonObject.addProperty(headers[j], values[j]);
      }
      String id = values[0];
      dataMap.put(id, jsonObject);
    }
    return dataMap;
  }

}
