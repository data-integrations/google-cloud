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

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;

/**
 * Information about Job
 */
public class Job implements Serializable {

  @SerializedName("name")
  @Expose
  private String name;
  @SerializedName("uid")
  @Expose
  private String uid;
  @SerializedName("startTime")
  @Expose
  private String startTime;
  @SerializedName("endTime")
  @Expose
  private String endTime;
  @SerializedName("state")
  @Expose
  private String state;
  @SerializedName("retryCount")
  @Expose
  private Integer retryCount;
  @SerializedName("service")
  @Expose
  private String service;
  @SerializedName("serviceJob")
  @Expose
  private String serviceJob;
  @SerializedName("message")
  @Expose
  private String message;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getUid() {
    return uid;
  }

  public void setUid(String uid) {
    this.uid = uid;
  }

  public String getStartTime() {
    return startTime;
  }

  public void setStartTime(String startTime) {
    this.startTime = startTime;
  }

  public String getEndTime() {
    return endTime;
  }

  public void setEndTime(String endTime) {
    this.endTime = endTime;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public Integer getRetryCount() {
    return retryCount;
  }

  public void setRetryCount(Integer retryCount) {
    this.retryCount = retryCount;
  }

  public String getService() {
    return service;
  }

  public void setService(String service) {
    this.service = service;
  }

  public String getServiceJob() {
    return serviceJob;
  }

  public void setServiceJob(String serviceJob) {
    this.serviceJob = serviceJob;
  }

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

}
