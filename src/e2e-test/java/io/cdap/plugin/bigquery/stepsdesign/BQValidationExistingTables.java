package io.cdap.plugin.bigquery.stepsdesign;

import com.esotericsoftware.minlog.Log;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cucumber.core.logging.Logger;
import io.cucumber.core.logging.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * BigQuery Plugin Existing Table validation.
 */
public class BQValidationExistingTables {

  private static final Logger LOG = LoggerFactory.getLogger(BQValidationExistingTables.class);
  private static final Gson gson = new Gson();

  /**
   * Validates the actual data in a BigQuery table against the expected data in a JSON file.
   * @param table    The name of the BigQuery table to retrieve data from.
   * @param fileName The name of the JSON file containing the expected data.
   * @return True if the actual data matches the expected data, false otherwise.
   */
  public static boolean validateActualDataToExpectedData(String table, String fileName) throws IOException,
    InterruptedException, URISyntaxException {
    Map<String, JsonObject> bigQueryMap = new HashMap<>();
    Map<String, JsonObject> fileMap = new HashMap<>();
    Path  bqExpectedFilePath = Paths.get(BQValidationExistingTables.class.getResource("/" + fileName).toURI());

    getBigQueryTableData(table, bigQueryMap);
    getFileData(bqExpectedFilePath.toString(), fileMap);
    boolean isMatched = bigQueryMap.equals(fileMap);
    return isMatched;
  }

  /**
   * Reads a JSON file line by line and populates a map with JSON objects using a specified ID key.
   *@param fileName The path to the JSON file to be read.
   * @param fileMap  A map where the extracted JSON objects will be stored with their ID values as keys.
   */

  public static void getFileData(String fileName, Map<String, JsonObject> fileMap) {
    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
      String line;
      while ((line = br.readLine()) != null) {
        JsonObject json = gson.fromJson(line, JsonObject.class);
        String idKey = getIdKey(json);
        if (idKey != null) {
          JsonElement idElement = json.get(idKey);
          if (idElement.isJsonPrimitive()) {
            String idValue = idElement.getAsString();
            fileMap.put(idValue, json);
          }
        } else {
          Log.error("ID key not found");
        }
      }
    } catch (IOException e) {
      Log.error("Error reading the file: " + e.getMessage());
    }
  }

  private static void getBigQueryTableData(String targetTable, Map<String, JsonObject> bigQueryMap)
    throws IOException, InterruptedException {
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + targetTable + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);

    for (FieldValueList row : result.iterateAll()) {
      JsonObject json = gson.fromJson(row.get(0).getStringValue(), JsonObject.class);
      String idKey = getIdKey(json); // Get the actual ID key from the JSON object
      if (idKey != null) {
        JsonElement idElement = json.get(idKey);
        if (idElement.isJsonPrimitive()) {
          String id = idElement.getAsString();
          bigQueryMap.put(id, json);
        } else {
          Log.error("Data Mismatched");
        }
      }
    }
  }

  /**
   * Retrieves the key for the ID element in the provided JSON object.
   *
   * @param json The JSON object to search for the ID key.
   */
  private static String getIdKey(JsonObject json) {
    if (json.has("ID")) {
      return "ID";
    } else if (json.has("Name")) {
      return "Name";
    } else if (json.has("Price")) {
      return "Price";
    } else if (json.has("Customer_Exists")) {
      return "Customer_Exists";
    } else {
      return null;
    }
  }
  }
