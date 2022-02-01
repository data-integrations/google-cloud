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

package io.cdap.plugin.gcp.bigquery.sqlengine;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.kms.v1.CryptoKeyName;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.engine.sql.SQLEngineException;
import io.cdap.plugin.common.ConfigUtil;
import io.cdap.plugin.gcp.bigquery.common.BigQueryBaseConfig;
import io.cdap.plugin.gcp.bigquery.connector.BigQueryConnectorConfig;
import io.cdap.plugin.gcp.bigquery.util.BigQueryUtil;
import io.cdap.plugin.gcp.common.CmekUtils;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Configuration for SQL Engine.
 */
public class BigQuerySQLEngineConfig extends BigQueryBaseConfig {

    public static final String NAME_LOCATION = "location";
    public static final String NAME_RETAIN_TABLES = "retainTables";
    public static final String NAME_TEMP_TABLE_TTL_HOURS = "tempTableTTLHours";
    public static final String NAME_JOB_PRIORITY = "jobPriority";
    public static final String NAME_USE_STORAGE_READ_API = "useStorageReadAPI";

    // Job priority options
    public static final String PRIORITY_BATCH = "batch";
    public static final String PRIORITY_INTERACTIVE = "interactive";
    private static final String SCHEME = "gs://";

    @Name(NAME_LOCATION)
    @Macro
    @Nullable
    @Description("The location where the BigQuery dataset will get created. " +
            "This value is ignored if the dataset or temporary bucket already exists.")
    protected String location;

    @Name(NAME_RETAIN_TABLES)
    @Macro
    @Nullable
    @Description("Select this option to retain all BigQuery temporary tables created during the pipeline run.")
    protected Boolean retainTables;

    @Name(NAME_TEMP_TABLE_TTL_HOURS)
    @Macro
    @Nullable
    @Description("Set table TTL for temporary BigQuery tables, in number of hours. Tables will be deleted " +
            "automatically on pipeline completion.")
    protected Integer tempTableTTLHours;

    @Name(NAME_JOB_PRIORITY)
    @Macro
    @Nullable
    @Description("Priority used to execute BigQuery Jobs. The value must be 'batch' or 'interactive'. " +
            "An interactive job is executed as soon as possible and counts towards the concurrent rate " +
            "limit and the daily rate limit. A batch job is queued and started as soon as idle resources " +
            "are available, usually within a few minutes. If the job hasn't started within 3 hours, " +
            "its priority is changed to 'interactive'")
    private String jobPriority;

    @Name(NAME_USE_STORAGE_READ_API)
    @Macro
    @Nullable
    @Description("Select this option to use the BigQuery Storage Read API when extracting records from BigQuery " +
            "during pipeline execution. This option can increase the performance of the BigQuery ELT Transformation " +
            "Pushdown execution. The usage of this API incurrs in additional costs. " +
            "This requires Scala version 2.12 to be installed in the execution environment.")
    private Boolean useStorageReadAPI;


    private BigQuerySQLEngineConfig(@Nullable BigQueryConnectorConfig connection,
                                    @Nullable String dataset, @Nullable String location,
                                    @Nullable String cmekKey, @Nullable String bucket) {
        super(connection, dataset, cmekKey, bucket);
        this.location = location;
    }

    private BigQuerySQLEngineConfig(@Nullable BigQueryConnectorConfig connection,
                                    @Nullable String datasetProject, @Nullable String dataset,
                                    @Nullable String location, @Nullable String cmekKey, @Nullable String bucket) {
        super(connection, dataset, cmekKey, bucket);
        this.location = location;
    }

    public Boolean shouldRetainTables() {
        return retainTables != null ? retainTables : false;
    }

    public Integer getTempTableTTLHours() {
        return tempTableTTLHours != null && tempTableTTLHours > 0 ? tempTableTTLHours : 72;
    }

    public Boolean shouldUseStorageReadAPI() {
        return useStorageReadAPI != null ? useStorageReadAPI : false;
    }

    public QueryJobConfiguration.Priority getJobPriority() {
        String priority = jobPriority != null ? jobPriority : "batch";
        return QueryJobConfiguration.Priority.valueOf(priority.toUpperCase());
    }

    /**
     * Validates configuration properties
     */
    public void validate() {
        // Ensure value for the job priority configuration property is valid
        if (jobPriority != null && !containsMacro(NAME_JOB_PRIORITY)
                && !PRIORITY_BATCH.equalsIgnoreCase(jobPriority)
                && !PRIORITY_INTERACTIVE.equalsIgnoreCase(jobPriority)) {
            throw new SQLEngineException("Property 'jobPriority' must be 'batch' or 'interactive'");
        }
    }

    public void validate(FailureCollector failureCollector) {
        validate(failureCollector, Collections.emptyMap());
    }

    public void validate(FailureCollector failureCollector, Map<String, String> arguments) {
        validate();
        ConfigUtil.validateConnection(this, useConnection, connection, failureCollector);
        String bucket = getBucket();
        if (!containsMacro(NAME_BUCKET)) {
            BigQueryUtil.validateBucket(bucket, NAME_BUCKET, failureCollector);
        }
        if (!containsMacro(NAME_DATASET)) {
            BigQueryUtil.validateDataset(dataset, NAME_DATASET, failureCollector);
        }
        if (!containsMacro(NAME_CMEK_KEY)) {
            validateCmekKey(failureCollector, arguments);
        }
    }

    void validateCmekKey(FailureCollector failureCollector, Map<String, String> arguments) {
        CryptoKeyName cmekKeyName = CmekUtils.getCmekKey(cmekKey, arguments, failureCollector);
        if (containsMacro(NAME_LOCATION)) {
            return;
        }
        validateCmekKeyLocation(cmekKeyName, null, location, failureCollector);
    }


    @Nullable
    public String getLocation() {
        return location;
    }

    public String getDatasetProject() {
        return connection == null ? null : connection.getDatasetProject();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * BigQuery SQlEngine configuration builder.
     */
    public static class Builder {
        private String project;
        private String dataset;
        private String cmekKey;
        private String location;
        private String bucket;
        private BigQueryConnectorConfig connection;

        public Builder setConnection(@Nullable BigQueryConnectorConfig connection) {
            this.connection = connection;
            return this;
        }

        public Builder setProject(@Nullable String project) {
            this.project = project;
            return this;
        }

        public Builder setDataset(@Nullable String dataset) {
            this.dataset = dataset;
            return this;
        }

        public Builder setCmekKey(@Nullable String cmekKey) {
            this.cmekKey = cmekKey;
            return this;
        }

        public Builder setLocation(@Nullable String location) {
            this.location = location;
            return this;
        }

        public Builder setBucket(@Nullable String bucket) {
            this.bucket = bucket;
            return this;
        }

        public BigQuerySQLEngineConfig build() {
            return new BigQuerySQLEngineConfig(
                    connection,
                    project,
                    dataset,
                    location,
                    cmekKey,
                    bucket
            );
        }
    }
}
