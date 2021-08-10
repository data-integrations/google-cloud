package io.cdap.plugin.gcp.dataplex.sink.connector;

import io.cdap.plugin.gcp.common.GCPConnectorConfig;

import javax.annotation.Nullable;

/**
 * Dataplex Connector configuration properties
 */
public class DataplexConnectorConfig extends GCPConnectorConfig {

    public DataplexConnectorConfig(@Nullable String project, @Nullable String serviceAccountType,
                                   @Nullable String serviceFilePath, @Nullable String serviceAccountJson) {
        super(project, serviceAccountType, serviceFilePath, serviceAccountJson);
    }
}
