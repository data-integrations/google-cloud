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
 * Information about Task
 */
public class Task implements Serializable {

  @SerializedName("trigger_spec")
  @Expose
  private TriggerSpec triggerSpec;
  @SerializedName("execution_spec")
  @Expose
  private ExecutionSpec executionSpec;
  @SerializedName("spark")
  @Expose
  private Spark spark;
  @SerializedName("description")
  @Expose
  private String description;

  public TriggerSpec getTriggerSpec() {
    return triggerSpec;
  }

  public void setTriggerSpec(TriggerSpec triggerSpec) {
    this.triggerSpec = triggerSpec;
  }

  public ExecutionSpec getExecutionSpec() {
    return executionSpec;
  }

  public void setExecutionSpec(ExecutionSpec executionSpec) {
    this.executionSpec = executionSpec;
  }

  public Spark getSpark() {
    return spark;
  }

  public void setSpark(Spark spark) {
    this.spark = spark;
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * Information about TriggerSpec
   */
  public static class TriggerSpec implements Serializable {
    @SerializedName("type")
    @Expose
    private String type;

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

  }

  /**
   * Information about Spark
   */
  public static class Spark implements Serializable {

    @SerializedName("sql_script")
    @Expose
    private String sqlScript;

    public String getSqlScript() {
      return sqlScript;
    }

    public void setSqlScript(String sqlScript) {
      this.sqlScript = sqlScript;
    }

  }

  /**
   * Information about ExecutionSpec
   */
  public static class ExecutionSpec implements Serializable {

    @SerializedName("args")
    @Expose
    private Args args;
    @SerializedName("service_account")
    @Expose
    private String serviceAccount;

    public Args getArgs() {
      return args;
    }

    public void setArgs(Args args) {
      this.args = args;
    }

    public String getServiceAccount() {
      return serviceAccount;
    }

    public void setServiceAccount(String serviceAccount) {
      this.serviceAccount = serviceAccount;
    }
  }

  /**
   * Information about Args
   */
  public static class Args implements Serializable {

    @SerializedName("TASK_ARGS")
    @Expose
    private String taskArgs;



    public String getTaskArgs() {
      return taskArgs;
    }

    public void setTaskArgs(String taskArgs) {
      this.taskArgs = taskArgs;
    }

  }
}
