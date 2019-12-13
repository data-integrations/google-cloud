

/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.plugin.gcp.dlp.configs;

import com.google.gson.Gson;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InfoTypeTransformations;
import com.google.privacy.dlp.v2.InfoTypeTransformations.InfoTypeTransformation;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.dlp.SensitiveDataMapping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


/**
 * TODO: Move to cdap-proto
 */
public final class DlpFieldTransformationConfig {

  private String transform;
  private String[] fields;
  private String[] filters;
  private DlpTransformConfig transformProperties;


  public FieldTransformation toFieldTransformation() {
    FieldTransformation.Builder fieldTransformationBuilder = FieldTransformation.newBuilder();

    //Adding target fields
    fieldTransformationBuilder.addAllFields(
      Arrays.stream(fields).map(field -> FieldId.newBuilder().setName(field).build()).collect(Collectors.toList())
    );

    if (fields.length == 0 || "NONE".equals(fields[0])) {
      fieldTransformationBuilder.setPrimitiveTransformation(transformProperties.toPrimitiveTransform());
    } else {

      SensitiveDataMapping sensitivityMapping = new SensitiveDataMapping();
      List<InfoType> sensitiveInfoTypes = sensitivityMapping.getSensitiveInfoTypes(filters);
      InfoTypeTransformation.Builder infoTypeTransformationBuilder = InfoTypeTransformations.InfoTypeTransformation
        .newBuilder();

      infoTypeTransformationBuilder.addAllInfoTypes(sensitiveInfoTypes);

      infoTypeTransformationBuilder.setPrimitiveTransformation(transformProperties.toPrimitiveTransform());

      fieldTransformationBuilder.setInfoTypeTransformations(
        InfoTypeTransformations.newBuilder().addTransformations(infoTypeTransformationBuilder));
    }

    return fieldTransformationBuilder.build();

  }


  DlpFieldTransformationConfig(String transform, String[] fields, String[] filters,
    DlpTransformConfig transformProperties) {
    this.transform = transform;
    this.fields = fields;
    this.filters = filters;
    this.transformProperties = transformProperties;
  }

  public void validate(FailureCollector collector, Schema inputSchema, final String widgetName) {
    // No need to validate 'transform' field since it is used to deserialize this object
    // So any invalid values would have been caused an error during deserialization

    Gson gson = new Gson();
    ErrorConfig errorConfig = getErrorConfig();
    if (fields.length == 0) {
      errorConfig.setTransformPropertyId("fields");
      collector.addFailure(String.format("No fields were selected to apply '%s' transform.", this.transform), "")
               .withConfigElement(widgetName, gson.toJson(errorConfig));
    }

    List<Schema.Type> supportedTypes = transformProperties.getSupportedTypes();

    for (String fieldName : this.fields) {
      Schema.Field field = inputSchema.getField(fieldName);
      if (field == null) {
        errorConfig.setTransformPropertyId("fields");
        collector.addFailure(String.format("Field '%s' is not present in the input schema", fieldName), "")
                 .withConfigElement(widgetName, gson.toJson(errorConfig));
      } else {

        Schema.Type fieldType =
          field.getSchema().isNullable() ? field.getSchema().getNonNullable().getType() : field.getSchema().getType();
        if (!supportedTypes.contains(fieldType)) {
          errorConfig.setTransformPropertyId("fields");
          collector
            .addFailure(String.format("Field '%s' has type '%s' which is not supported by '%s' transform", fieldName,
                                      fieldType.toString(), this.transform), "")
            .withConfigElement(widgetName, gson.toJson(errorConfig));
        }

      }


    }

    if (filters.length == 0) {
      errorConfig.setTransformPropertyId("filters");
      collector.addFailure("At least one filter must be selected.", "")
               .withConfigElement(widgetName, gson.toJson(errorConfig));
    }

    transformProperties.validate(collector, widgetName, getErrorConfig());
  }

  public String getTransform() {
    return transform;
  }

  public String[] getFields() {
    return fields;
  }

  public String[] getFilters() {
    return filters;
  }

  public DlpTransformConfig getTransformProperties() {
    return transformProperties;
  }

  public ErrorConfig getErrorConfig() {
    return getErrorConfig(null);
  }

  public ErrorConfig getErrorConfig(String transformPropertyId) {
    return new ErrorConfig(this, transformPropertyId, false);
  }

  public List<String> getFilterDisplayNames() {
    List<String> names = new ArrayList<>();

    for (String filter : filters) {
      if (filter.equals("NONE")) {
        filter = "Custom Template";
      } else {
        filter = Arrays.stream(filter.toLowerCase().split(" ")).map(s -> s.toUpperCase().charAt(0) + s.substring(1))
                       .collect(
                         Collectors.joining(" "));

      }
      names.add(filter);
    }
    return names;
  }
}


