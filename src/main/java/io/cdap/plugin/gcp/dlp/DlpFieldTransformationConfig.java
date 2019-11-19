

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

package io.cdap.plugin.gcp.dlp;

import com.google.common.base.Strings;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InfoTypeTransformations;
import com.google.privacy.dlp.v2.InfoTypeTransformations.InfoTypeTransformation;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.dlp.configs.DlpTransformConfig;

import java.util.List;


/**
 * TODO: Move to cdap-proto
 */
final class DlpFieldTransformationConfig {

  private String transform;
  private String[] fields;
  private String[] filters;
  private DlpTransformConfig transformProperties;

  public FieldTransformation toFieldTransformation() {
    FieldTransformation.Builder fieldTransformationBuilder = FieldTransformation.newBuilder();

    //Adding target fields
    for (int i = 0; i < fields.length; i++) {
      fieldTransformationBuilder.setFields(i, FieldId.newBuilder().setName(fields[i]).build());
    }

    if (fields.length == 0 && "NONE".equals(fields[0])) {
      fieldTransformationBuilder.setPrimitiveTransformation(transformProperties.toPrimitiveTransform());
    } else {

      SensitiveDataMapping sensitivityMapping = new SensitiveDataMapping();
      List<InfoType> sensitiveInfoTypes = sensitivityMapping.getSensitiveInfoTypes(filters);
      InfoTypeTransformation.Builder infoTypeTransformationBuilder = InfoTypeTransformations.InfoTypeTransformation
        .newBuilder();

      for (int i = 0; i < sensitiveInfoTypes.size(); i++) {
        infoTypeTransformationBuilder.setInfoTypes(i, sensitiveInfoTypes.get(i));
      }

      infoTypeTransformationBuilder.setPrimitiveTransformation(transformProperties.toPrimitiveTransform());

      fieldTransformationBuilder.setInfoTypeTransformations(
        InfoTypeTransformations.newBuilder().setTransformations(0, infoTypeTransformationBuilder));
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

  public void validate(FailureCollector collector, Schema inputSchema) {
    // No need to validate 'transform' field since it is used to deserialize this object
    // So any invalid values would have been caused an error during deserialization

    for (String field : this.fields) {
      if (inputSchema.getField(field) == null) {
        collector.addFailure(String.format("Field '%s' is not present in the input schema"), "");
      }
      //TODO Add field type checking
    }

    if (filters.length == 0) {
      collector.addFailure("At least one filter must be selected.", "");
    }

    transformProperties.validate(collector);
  }
}


