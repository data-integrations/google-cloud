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

import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.dataplex.common.util.DataplexUtil;

/**
 * Information about Entity
 */
public class Entity {

  private String name;
  private String id;
  private String type;
  private String asset;
  private String dataPath;
  private String system;
  private String createTime;
  private String updateTime;
  private Schema schema;
  private Format format;


  public io.cdap.cdap.api.data.schema.Schema getSchema(FailureCollector collector) {
    return DataplexUtil.getTableSchema(this.schema, collector);
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public Format getFormat() {
    return format;
  }

  public void setFormat(Format format) {
    this.format = format;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public String getAsset() {
    return asset;
  }

  public void setAsset(String asset) {
    this.asset = asset;
  }

  public String getDataPath() {
    return dataPath;
  }

  public void setDataPath(String dataPath) {
    this.dataPath = dataPath;
  }

  public String getSystem() {
    return system;
  }

  public void setSystem(String system) {
    this.system = system;
  }

  public String getCreateTime() {
    return createTime;
  }

  public void setCreateTime(String createTime) {
    this.createTime = createTime;
  }

  public String getUpdateTime() {
    return updateTime;
  }

  public void setUpdateTime(String updateTime) {
    this.updateTime = updateTime;
  }

}
