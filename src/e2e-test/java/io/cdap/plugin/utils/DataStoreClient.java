package io.cdap.plugin.utils;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Represents DataStore client.
 */
public class DataStoreClient {
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
      .set("hireDate", String.valueOf(new Date()))
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
}
