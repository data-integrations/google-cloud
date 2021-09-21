package io.cdap.plugin.gcp.common;

import com.google.api.pathtemplate.ValidationException;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Arguments;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.gcp.gcs.GCSPath;

import java.io.IOException;
import javax.annotation.Nullable;

/**
 * Contains config properties common to all GCP plugins, like project id and service account key.
 */
public class GCPConfig extends PluginConfig {
  public static final String NAME_PROJECT = "project";
  public static final String NAME_SERVICE_ACCOUNT_TYPE = "serviceAccountType";
  public static final String NAME_SERVICE_ACCOUNT_FILE_PATH = "serviceFilePath";
  public static final String NAME_SERVICE_ACCOUNT_JSON = "serviceAccountJSON";
  public static final String AUTO_DETECT = "auto-detect";
  public static final String SERVICE_ACCOUNT_FILE_PATH = "filePath";
  public static final String SERVICE_ACCOUNT_JSON = "JSON";
  public static final String NAME_CMEK_KEY = "cmekKey";

  @Name(NAME_PROJECT)
  @Description("Google Cloud Project ID, which uniquely identifies a project. "
    + "It can be found on the Dashboard in the Google Cloud Platform Console.")
  @Macro
  @Nullable
  protected String project;

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

  public String getProject() {
    String projectId = tryGetProject();
    if (projectId == null) {
      throw new IllegalArgumentException(
        "Could not detect Google Cloud project id from the environment. Please specify a project id.");
    }
    return projectId;
  }

  @Nullable
  public String tryGetProject() {
    if (containsMacro(NAME_PROJECT) && Strings.isNullOrEmpty(project)) {
      return null;
    }
    String projectId = project;
    if (Strings.isNullOrEmpty(project) || AUTO_DETECT.equals(project)) {
      projectId = ServiceOptions.getDefaultProjectId();
    }
    return projectId;
  }

  @Nullable
  public String getServiceAccountFilePath() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_FILE_PATH) || serviceFilePath == null ||
      serviceFilePath.isEmpty() || AUTO_DETECT.equals(serviceFilePath)) {
      return null;
    }
    return serviceFilePath;
  }

  @Nullable
  public String getServiceAccountJson() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_JSON) || Strings.isNullOrEmpty(serviceAccountJson)) {
      return null;
    }
    return serviceAccountJson;
  }

  /**
   * @return Service Account Type, defaults to filePath.
   */
  @Nullable
  public String getServiceAccountType() {
    if (containsMacro(NAME_SERVICE_ACCOUNT_TYPE)) {
      return null;
    }
    return Strings.isNullOrEmpty(serviceAccountType) ? SERVICE_ACCOUNT_FILE_PATH : serviceAccountType;
  }

  @Nullable
  public Boolean isServiceAccountJson() {
    String serviceAccountType = getServiceAccountType();
    return Strings.isNullOrEmpty(serviceAccountType) ? null : serviceAccountType.equals(SERVICE_ACCOUNT_JSON);
  }

  @Nullable
  public Boolean isServiceAccountFilePath() {
    String serviceAccountType = getServiceAccountType();
    return Strings.isNullOrEmpty(serviceAccountType) ? null : serviceAccountType.equals(SERVICE_ACCOUNT_FILE_PATH);
  }

  @Nullable
  public String getServiceAccount() {
    Boolean serviceAccountJson = isServiceAccountJson();
    if (serviceAccountJson == null) {
      return null;
    }
    return serviceAccountJson ? getServiceAccountJson() : getServiceAccountFilePath();
  }

  /**
   * Return true if the service account is set to auto-detect but it can't be fetched from the environment.
   * This shouldn't result in a deployment failure, as the credential could be detected at runtime if the pipeline
   * runs on dataproc. This should primarily be used to check whether certain validation logic should be skipped.
   *
   * @return true if the service account is set to auto-detect but it can't be fetched from the environment.
   */
  public boolean autoServiceAccountUnavailable() {
    if (getServiceAccountFilePath() == null && SERVICE_ACCOUNT_FILE_PATH.equals(getServiceAccountType())) {
      try {
        ServiceAccountCredentials.getApplicationDefault();
      } catch (IOException e) {
        return true;
      }
    }
    return false;
  }

  public String getCmekKey(Arguments arguments) {
    String configKey = getProperties().getProperties().get(NAME_CMEK_KEY);
    if (!Strings.isNullOrEmpty(configKey)) {
      return configKey;
    }
    return arguments.get("gcp.cmek.key.name");
  }

  public CryptoKeyName parseCmekKey(String key, FailureCollector collector) {
    CryptoKeyName cmekKeyName = null;
    try {
      cmekKeyName = CryptoKeyName.parse(key);
    } catch (ValidationException e) {
      collector.addFailure(e.getMessage(), null)
        .withConfigProperty(NAME_CMEK_KEY).withStacktrace(e.getStackTrace());
    }
    return cmekKeyName;
  }

  public Storage getStorage() {
    Boolean isServiceAccountFilePath = isServiceAccountFilePath();
    Credentials credentials = null;
    try {
      credentials = getServiceAccount() == null ?
        null : GCPUtils.loadServiceAccountCredentials(getServiceAccount(), isServiceAccountFilePath);
    } catch (Exception e) {
      /* Ignoring the exception because we don't want to highlight config if an exception occurs while
        loading credentials */
      return null;
    }
    return GCPUtils.getStorage(getProject(), credentials);
  }

  public boolean validateCmekKeyAndBucketLocation(Storage storage, String path, CryptoKeyName cmekKey,
                                                  @Nullable String location, FailureCollector collector) {
    GCSPath gcsPath = GCSPath.from(path);
    Bucket bucket = null;
    try {
      bucket = storage.get(gcsPath.getBucket());
    } catch (StorageException e) {
          /* Ignoring the exception because we don't want the validation to fail if there is an exception getting
          the bucket information either because the service account used during validation can be different than
          the service account that will be used at runtime (the dataproc service account)
          (assuming the user has auto-detect for the service account) */
      return false;
    }
    if (bucket == null) {
      String cmekKeyLocation = cmekKey.getLocation();
      if ((Strings.isNullOrEmpty(location) && !"US".equalsIgnoreCase(cmekKeyLocation))
        || (!Strings.isNullOrEmpty(location) && !cmekKeyLocation.equalsIgnoreCase(location))) {
        String bucketLocation = location;
        if (Strings.isNullOrEmpty(bucketLocation)) {
          bucketLocation = "US";
        }
        collector.addFailure(String.format("CMEK key '%s' is in location '%s' while the GCS bucket '%s' will"
                                             + " be created in location '%s'.", cmekKey,
                                           cmekKeyLocation, gcsPath.getBucket(), bucketLocation)
          , "Modify the CMEK key or bucket location to be the same")
          .withConfigProperty(NAME_CMEK_KEY);
      }
    }
    return true;
  }
}
