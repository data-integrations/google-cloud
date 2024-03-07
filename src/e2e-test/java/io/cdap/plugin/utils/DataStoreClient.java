package io.cdap.plugin.utils;

import com.esotericsoftware.minlog.Log;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents DataStore client.
 */
public class DataStoreClient {
  static Gson gson = new Gson();
  private static final Logger logger = LoggerFactory.getLogger(PubSubClient.class);
  static Datastore datastore = DatastoreOptions.newBuilder().
                               setProjectId(PluginPropertyUtils.pluginProp("projectId")).build().getService();
  static Key key;

  /**
   * Creates a new entity of the specified kind in Google Cloud Datastore.
   *
   * @param kindName the kind name for the entity to be created
   * @return the kind name of the created entity
   */
  public static String createKind(String kindName) {
    KeyFactory keyFactory = datastore.newKeyFactory().setKind(kindName);
    // Create an incomplete key (it will be auto-generated when saved)
    IncompleteKey incompleteKey = keyFactory.newKey();
    // Build the entity
    FullEntity<IncompleteKey> entity = Entity.newBuilder(incompleteKey)
      .set("firstName", PluginPropertyUtils.pluginProp("name"))
      .set("age", Integer.parseInt(PluginPropertyUtils.pluginProp("age")))
      .set("isValid", PluginPropertyUtils.pluginProp("result").isEmpty())
      .set("postalAdd", Float.parseFloat(PluginPropertyUtils.pluginProp("address")))
      // Add other properties as needed
      .build();

    // Save the entity
    Entity savedEntity = datastore.put(entity);
    key = savedEntity.getKey();
    logger.info("Entity saved with key: " + key);

    return kindName;
  }

  /**
   * Deletes all entities of the specified kind from Google Cloud Datastore.
   *
   * @param kindName the kind name of the entities to be deleted
   */
  public static void deleteEntity(String kindName) {
    Query<Entity> query = Query.newEntityQueryBuilder()
      .setKind(kindName)
      .build();
    // Execute the query
    QueryResults<Entity> queryResults = datastore.run(query);
    // Delete each entity
    while (queryResults.hasNext()) {
      Entity entity = queryResults.next();
      Key entityKey = entity.getKey();
      datastore.delete(entityKey);
      logger.info("Entity deleted: " + entityKey);
    }

    logger.info("All entities of kind '" + kindName + "' deleted successfully.");
  }

  /**
   * Returns the key-literal representation of the current entity key.
   *
   * @return the key-literal representation of the current entity key
   */
  public static String getKeyLiteral() {
    String kind = key.getKind();  // Get the kind of the entity
    long id = key.getId();        // Get the ID of the entity
    String keyLiteral = String.format("Key(%s, %d)", kind, id);

    return keyLiteral;
  }

public static boolean validateActualDataToExpectedData(String kindName, String fileName) throws URISyntaxException {
  Map<String, JsonObject> datastoreMap;
  Map<String, JsonObject> fileMap = new HashMap<>();
  Path importExpectedFile = Paths.get(DataStoreClient.class.getResource("/" + fileName).toURI());
  datastoreMap = fetchEntities(kindName);
  getFileData(importExpectedFile.toString(), fileMap);

  boolean isMatched = datastoreMap.equals(fileMap);

  return isMatched;
}

  public static Map<String, JsonObject> fetchEntities(String kindName) {
    Map<String, JsonObject> entityMap = new HashMap<>();

    Query<Entity> query = Query.newEntityQueryBuilder().setKind(kindName).build();
    QueryResults<Entity> results = datastore.run(query);

    while (results.hasNext()) {
      Entity entity = results.next();
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("firstName", entity.getString("firstName"));
      jsonObject.addProperty("age", entity.getLong("age"));
      jsonObject.addProperty("isValid", entity.getBoolean("isValid"));
      jsonObject.addProperty("postalAdd", entity.getDouble("postalAdd"));

      // Convert JsonObject to String and store it in the map based on age
      String ageKey = String.valueOf(entity.getLong("age"));
      entityMap.put(ageKey, jsonObject);
    }

    return entityMap;
  }

  public static void getFileData(String fileName, Map<String, JsonObject> fileMap) {
    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
      String line;
      while ((line = br.readLine()) != null) {
        JsonObject json = gson.fromJson(line, JsonObject.class);
        if (json.has("age")) { // Check if the JSON object has the "id" key
          JsonElement idElement = json.get("age");
          if (idElement.isJsonPrimitive()) {
            String idKey = idElement.getAsString();
            fileMap.put(idKey, json);
          } else {
            Log.error("age key not found");
          }
        }
      }
    } catch (IOException e) {
      System.err.println("Error reading the file: " + e.getMessage());
    }
  }
}
