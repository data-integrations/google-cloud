package co.cask.gcp.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Contains config properties common to all GCP plugins, like project id and service account key.
 */
public class GCPConfig extends PluginConfig {
  public static final String PROJECT_DESC = "The Google Cloud Project ID, which uniquely identifies a project. "
    + "It can be found on the Dashboard in the Google Cloud Platform Console.";
  public static final String SERVICE_ACCOUNT_DESC = "Path on the local file system of the service account key used "
    + "for authorization. Does not need to be specified when running on a Dataproc cluster. "
    + "When running on other clusters, the file must be present on every node in the cluster.";

  @Name("project")
  @Description(PROJECT_DESC)
  @Macro
  @Nullable
  public String project;

  @Name("serviceFilePath")
  @Description(SERVICE_ACCOUNT_DESC)
  @Macro
  @Nullable
  public String serviceAccountFilePath;
}
