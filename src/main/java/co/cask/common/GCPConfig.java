package co.cask.common;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Contains config properties common to all GCP plugins, like project id and service account key.
 */
public class GCPConfig extends PluginConfig {
  @Name("project")
  @Description("The Google Cloud Project ID, which uniquely identifies a project. "
    + "It can be found on the Dashboard in the Google Cloud Platform Console.")
  @Macro
  @Nullable
  public String project;

  @Name("serviceFilePath")
  @Description("Path on the local file system of the service account key used for authorization. "
    + "Does not need to be specified when running on a Dataproc cluster. "
    + "When running on other clusters, the file must be present on every node in the cluster.")
  @Macro
  @Nullable
  public String serviceAccountFilePath;
}
