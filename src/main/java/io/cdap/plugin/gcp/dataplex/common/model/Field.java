/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.gcp.dataplex.common.model;

import com.google.cloud.bigquery.Field.Mode;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexTypeName;

import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

/**
 * The type Field.
 */
public class Field {
  private String name;
  private DataplexTypeName type;
  private Mode mode;
  @JsonIgnore
  private List<Field> subFields;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public DataplexTypeName getType() {
    return type;
  }

  public void setType(DataplexTypeName type) {
    this.type = type;
  }

  public Mode getMode() {
    return mode;
  }

  public void setMode(Mode mode) {
    this.mode = mode;
  }

  public List<Field> getSubFields() {
    return subFields;
  }

  public void setSubFields(List<Field> subFields) {
    this.subFields = subFields;
  }
}
