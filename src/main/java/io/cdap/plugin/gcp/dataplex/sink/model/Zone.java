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

package io.cdap.plugin.gcp.dataplex.sink.model;

/**
 * Holds Zone details
 */
public class Zone {
  public String name;
  public String displayName;
  public String uid;
  public String createTime;
  public String updateTime;
  public String state;
  public String type;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getDisplayName() {
    return displayName;
  }

  public void setDisplayName(String displayName) {
    this.displayName = displayName;
  }

  public String getUid() {
    return uid;
  }

  public void setUid(String uid) {
    this.uid = uid;
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

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }
}
