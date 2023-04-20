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
package io.cdap.plugin.gcp.publisher.source;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.publisher.PubSubConstants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Extension to the PubSubSubscriberConfig class with additional fields related to record schema.
 */
public class GoogleSubscriberConfig extends PubSubSubscriberConfig implements Serializable {

  private static final String SCHEMA = "schema";
  private static final Schema DEFAULT_SCHEMA =
    Schema.recordOf("event",
                    Schema.Field.of("message", Schema.of(Schema.Type.BYTES)),
                    Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                    Schema.Field.of("timestamp", Schema.of(Schema.LogicalType.TIMESTAMP_MICROS)),
                    Schema.Field.of("attributes", Schema.mapOf(Schema.of(Schema.Type.STRING),
                                                               Schema.of(Schema.Type.STRING)))
    );

  @Macro
  @Nullable
  @Description("Format of the data to read. Supported formats are 'avro', 'blob', 'tsv', 'csv', 'delimited', 'json', "
    + "'parquet' and 'text'.")
  protected String format;

  @Description("The delimiter to use if the format is 'delimited'. The delimiter will be ignored if the format "
    + "is anything other than 'delimited'.")
  @Macro
  @Nullable
  protected String delimiter;

  @Name(SCHEMA)
  @Macro
  @Nullable
  protected String schema;

  @Override
  public void validate(FailureCollector collector) {
    super.validate(collector);

    final Schema outputSchema = getSchema();
    final ArrayList<String> defaultSchemaFields = getFieldsOfDefaultSchema();
    ArrayList<String> outputSchemaFields = new ArrayList<>();

    if (outputSchema != null) {
      for (Schema.Field field : Objects.requireNonNull(outputSchema.getFields())) {
        outputSchemaFields.add(field.getName());
      }

      for (Schema.Field field : Objects.requireNonNull(DEFAULT_SCHEMA.getFields())) {
        if (!outputSchemaFields.contains(field.getName())) {
          collector.addFailure("Some required fields are missing from the schema.",
                               String.format("You should use the existing fields of default schema %s.",
                                             defaultSchemaFields))
            .withConfigProperty(schema);
        }
      }

      for (Schema.Field field : Objects.requireNonNull(outputSchema.getFields())) {
        Schema.Field outputField = DEFAULT_SCHEMA.getField(field.getName());
        if (field.getSchema().isNullable()) {
          collector.addFailure(String.format("Null is not allowed in %s.", field.getName()),
                               "Schema is non-nullable")
            .withConfigProperty(schema);
        }
        if (outputField == null) {
          collector.addFailure(String.format("Field %s is not allowed.", field.getName()),
                               "You should use the existing fields of default schema.")
            .withConfigProperty(schema);
        } else {
          Schema.Type fieldType = field.getSchema().getType();
          if (field.getName().equals("message")) {
            if (!(fieldType == Schema.Type.RECORD || fieldType == Schema.Type.BYTES)) {
              collector.addFailure(String.format("Type %s is not allowed in %s.",
                                                 fieldType.toString().toLowerCase(), field.getName()),
                                   "Type should be record or byte.")
                .withConfigProperty(schema);
            }
            continue;
          }
          if (!fieldType.equals(outputField.getSchema().getType())) {
            collector.addFailure(String.format("Type %s is not allowed in %s.",
                                               fieldType.toString().toLowerCase(), field.getName()),
                                 String.format("You should use the same type [%s] as in default schema.",
                                               outputField.getSchema().toString()))
              .withConfigProperty(schema);
          }
        }
      }
    }

    if (!containsMacro(PubSubConstants.DELIMITER) && (!containsMacro(PubSubConstants.FORMAT) &&
      getFormat().equalsIgnoreCase(PubSubConstants.DELIMITED) && delimiter == null)) {
      collector.addFailure(String.format("Delimiter is required when format is set to %s.", getFormat()),
                           "Ensure the delimiter is provided.")
        .withConfigProperty(delimiter);
    }

    collector.getOrThrowException();
  }

  public String getFormat() {
    return Strings.isNullOrEmpty(format) ? PubSubConstants.TEXT : format;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public ArrayList<String> getFieldsOfDefaultSchema() {
    ArrayList<String> outputSchemaAttributes = new ArrayList<>();
    for (Schema.Field field : Objects.requireNonNull(DEFAULT_SCHEMA.getFields())) {
      outputSchemaAttributes.add(field.getName());
    }
    return outputSchemaAttributes;
  }

  public Schema getSchema() {
    try {
      if (containsMacro(SCHEMA)) {
        return null;
      }
      return Strings.isNullOrEmpty(schema) ? DEFAULT_SCHEMA : Schema.parseJson(schema);
    } catch (Exception e) {
      throw new IllegalArgumentException(String.format("Unable to parse schema with error %s, %s",
                                                       e.getMessage(), e));
    }
  }
}
