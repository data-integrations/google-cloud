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

package io.cdap.plugin.gcp.gcs.actions;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Joiner;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.cdap.plugin.gcp.gcs.GCSPath;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class <code>GCSArgumentSetter</code> get json file configuration from GCS.
 *
 * <p>The plugin provides the ability to map json properties as pipeline arguments name and columns
 * values as pipeline arguments <code>
 *   {
 *     "arguments" : [
 *        { "name" : "input.path", "type" : "string", "value" : "/data/sunny_feeds/master"},
 *        { "name" : "parse.schema, "type" : "schema", "value" : [
 *             { "name" : "fname", "type" : "string", "nullable" : true },
 *             { "name" : "age", "type" : "int", "nullable" : true},
 *             { "name" : "salary", "type" : "float", "nullable" : false}
 *          ]},
 *        { "name" : "directives", "type" : "array", "value" : [
 *             "parse-as-json body",
 *             "columns-replace s/body_//g",
 *             "keep f1,f2"
 *          ]}
 *     ]
 *   }
 *
 *   In case when `nullable` is not specified inside field json, then it defaults to true.
 * </code>
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name(GCSArgumentSetter.NAME)
@Description("Argument setter for dynamically configuring pipeline from GCS")
public final class GCSArgumentSetter extends Action {

  public static final String NAME = "GCSArgumentSetter";
  private GCSArgumentSetterConfig config;

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    super.configurePipeline(configurer);
    StageConfigurer stageConfigurer = configurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    config.validate(collector);
  }

  @Override
  public void run(ActionContext context) {
    config.validateProperties(context.getFailureCollector());
    String fileContent = GCSArgumentSetter.getContent(config);

    try {
      Configuration configuration =
          new GsonBuilder().create().fromJson(fileContent, Configuration.class);
      for (Argument argument : configuration.getArguments()) {
        String name = argument.getName();
        String value = argument.getValue();
        if (value != null) {
          context.getArguments().set(name, value);
        } else {
          String errorReason = String.format("Configuration '%s' is null. Cannot set argument to null.", name);
          throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
            errorReason, errorReason, ErrorType.USER, false, null);
        }
      }
    } catch (JsonSyntaxException e) {
      String errorReason = String.format("Could not parse response from '%s': %s", config.getPath(), e.getMessage());
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorReason, ErrorType.USER, false, null);
    }
  }

  private static Storage getStorage(GCSArgumentSetterConfig config) {
    String serviceAccount = config.getServiceAccount();
    StorageOptions.Builder builder = StorageOptions.newBuilder().setProjectId(config.getProject());
    if (serviceAccount != null) {
      try {
        builder.setCredentials(
          GCPUtils.loadServiceAccountCredentials(serviceAccount, config.isServiceAccountFilePath()));
      } catch (IOException e) {
        String errorReason = "Failed to load service account credentials.";
        String errorMessage = String.format("%s: %s", e.getClass(), e.getMessage());
        throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
          errorReason, errorMessage, ErrorType.USER, false, e);
      }
    }
    return builder.build().getService();
  }

  public static String getContent(GCSArgumentSetterConfig config) {
    Storage storage = getStorage(config);
    GCSPath path = config.getPath();
    Blob blob = storage.get(path.getBucket(), path.getName());
    return new String(blob.getContent());
  }

  private static final class Configuration {

    private List<Argument> arguments;

    public List<Argument> getArguments() {
      return arguments;
    }
  }

  private static final class Argument {

    private String name;
    private String type;
    private JsonElement value;

    Argument() {
      type = "string";
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getValue() {
      if (value == null) {
        String errorReason = String.format("Null Argument value for name '%s'", name);
        throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
          errorReason, errorReason, ErrorType.USER, false, null);
      }
      if (type.equalsIgnoreCase("schema")) {
        return createSchema(value).toString();
      } else if (type.equalsIgnoreCase("int")) {
        return Integer.toString(value.getAsInt());
      } else if (type.equalsIgnoreCase("float")) {
        return Float.toString(value.getAsFloat());
      } else if (type.equalsIgnoreCase("double")) {
        return Double.toString(value.getAsDouble());
      } else if (type.equalsIgnoreCase("short")) {
        return Short.toString(value.getAsShort());
      } else if (type.equalsIgnoreCase("string")) {
        return value.getAsString();
      } else if (type.equalsIgnoreCase("char")) {
        return Character.toString(value.getAsCharacter());
      } else if (type.equalsIgnoreCase("array")) {
        List<String> values = new ArrayList<>();
        for (JsonElement v : value.getAsJsonArray()) {
          values.add(v.getAsString());
        }
        return Joiner.on("\n").join(values);
      } else if (type.equalsIgnoreCase("map")) {
        List<String> values = new ArrayList<>();
        for (Map.Entry<String, JsonElement> entry : value.getAsJsonObject().entrySet()) {
          values.add(String.format("%s=%s", entry.getKey(), entry.getValue().getAsString()));
        }
        return Joiner.on(";").join(values);
      }

      String errorReason = String.format("Invalid argument value '%s'", value);
      throw ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
        errorReason, errorReason, ErrorType.USER, false, null);
    }

    private Schema createSchema(JsonElement array) {
      List<Schema.Field> fields = new ArrayList<>();
      for (JsonElement field : array.getAsJsonArray()) {
        Schema.Type type = Schema.Type.STRING;

        JsonObject object = field.getAsJsonObject();
        String fieldType = object.get("type").getAsString().toLowerCase();

        boolean isNullable = true;
        if (object.get("nullable") != null) {
          isNullable = object.get("nullable").getAsBoolean();
        }

        String name = object.get("name").getAsString();

        if (fieldType.equals("double")) {
          type = Schema.Type.DOUBLE;
        } else if (fieldType.equals("float")) {
          type = Schema.Type.FLOAT;
        } else if (fieldType.equals("long")) {
          type = Schema.Type.LONG;
        } else if (fieldType.equals("int")) {
          type = Schema.Type.INT;
        } else if (fieldType.equals("short")) {
          type = Schema.Type.INT;
        } else if (fieldType.equals("string")) {
          type = Schema.Type.STRING;
        }

        Schema schema;
        if (isNullable) {
          schema = Schema.nullableOf(Schema.of(type));
        } else {
          schema = Schema.of(type);
        }
        Schema.Field fld = Schema.Field.of(name, schema);
        fields.add(fld);
      }

      return Schema.recordOf("record", fields);
    }
  }
}
