package io.cdap.plugin.gcp.dlp.configs;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import java.lang.reflect.Type;
import java.util.HashMap;

/**
 * Custom codec for decoding DlpTansform
 */
public class DlpFieldTransformationConfigCodec implements JsonSerializer<DlpFieldTransformationConfig>,
  JsonDeserializer<DlpFieldTransformationConfig> {

  private final HashMap<String, Type> transformTypeDictionary = new HashMap<String, Type>() {{
    put("MASKING", MaskingTransformConfig.class);
  }};

  @Override
  public JsonElement serialize(DlpFieldTransformationConfig src, Type typeOfSrc, JsonSerializationContext context) {
    return null;
  }

  @Override
  public DlpFieldTransformationConfig deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    String transform = jsonObj.get("transform").getAsString();
    String fieldsString = context.deserialize(jsonObj.get("fields"), String.class);
    String[] fields = new String[]{};
    if (fieldsString.length() > 0) {
      fields = fieldsString.split(",");
    }

    String filtersString = context.deserialize(jsonObj.get("filters"), String.class);
    String[] filters = new String[]{};
    if (filtersString.length() > 0) {
      filters = filtersString.split(",");
    }

    DlpTransformConfig transformProperties = context
      .deserialize(jsonObj.get("transformProperties"), transformTypeDictionary.get(transform));

    return new DlpFieldTransformationConfig(transform, fields, filters, transformProperties);
  }

}
