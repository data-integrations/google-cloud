package io.cdap.plugin.gcp.bigquery.source;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 *
 */
@Name("BigQuery")
@Plugin(type = "connector")
public class BigQueryConnector {

  private Config config;

  /**
   * 
   */
  public final class Config extends PluginConfig {
    public static final String NAME_PROJECT = "project";
    public static final String NAME_SERVICE_ACCOUNT_TYPE = "serviceAccountType";
    public static final String NAME_SERVICE_ACCOUNT_FILE_PATH = "serviceFilePath";
    public static final String NAME_SERVICE_ACCOUNT_JSON = "serviceAccountJSON";
    public static final String AUTO_DETECT = "auto-detect";
    public static final String SERVICE_ACCOUNT_FILE_PATH = "filePath";
    public static final String SERVICE_ACCOUNT_JSON = "JSON";
    public static final String NAME_DATASET_PROJECT = "datasetProject";

    @Name(NAME_PROJECT)
    @Description("Google Cloud Project ID, which uniquely identifies a project. "
                   + "It can be found on the Dashboard in the Google Cloud Platform Console.")
    @Macro
    @Nullable
    protected String project;

    @Name(NAME_DATASET_PROJECT)
    @Macro
    @Nullable
    @Description("The project the dataset belongs to. This is only required if the dataset is not "
                   + "in the same project that the BigQuery job will run in. If no value is given, it will" +
                   " default to the configured "
                   + "project ID.")
    private String datasetProject;

    @Name(NAME_SERVICE_ACCOUNT_TYPE)
    @Description("Service account type, file path where the service account is located or the JSON content of the " +
                   "service account.")
    @Macro
    @Nullable
    protected String serviceAccountType;

    @Name(NAME_SERVICE_ACCOUNT_FILE_PATH)
    @Description("Path on the local file system of the service account key used "
                   + "for authorization. Can be set to 'auto-detect' when running on a Dataproc cluster. "
                   + "When running on other clusters, the file must be present on every node in the cluster.")
    @Macro
    @Nullable
    protected String serviceFilePath;

    @Name(NAME_SERVICE_ACCOUNT_JSON)
    @Description("Content of the service account file.")
    @Macro
    @Nullable
    protected String serviceAccountJson;
  }
}
