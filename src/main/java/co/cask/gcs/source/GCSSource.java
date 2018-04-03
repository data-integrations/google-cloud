/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.gcs.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Class description here.
 */
@Plugin(type = "batchsource")
@Name(GCSSource.NAME)
@Description(GCSSource.DESCRIPTION)
public class GCSSource extends AbstractFileBatchSource {
  private static final Logger LOG = LoggerFactory.getLogger(GCSSource.class);
  public static final String NAME = "GCSFile";
  public static final String DESCRIPTION = "Source for Google Cloud Storage.";
  private final GCSSourceConfig config;

  public GCSSource(GCSSourceConfig config) {
    super(config);
    this.config = config;
  }


  public static class GCSSourceConfig extends FileSourceConfig {
    @Name("path")
    @Description("Google Cloud Storage File path")
    @Macro
    public String path;

    @Name("project")
    @Description("Project ID")
    @Macro
    public String project;

    @Name("serviceFilePath")
    @Description("Service account file path.")
    @Macro
    public String serviceAccountFilePath;

    @Name("bucket")
    @Description("Name of the bucket.")
    @Macro
    public String bucket;

    @Override
    protected void validate() {
      super.validate();
      if (!containsMacro("path") && (!path.startsWith("gs://"))) {
        throw new IllegalArgumentException("Path must start with gs:// for Google Cloud Storage (GCS).");
      }
    }

    @Override
    protected Map<String, String> getFileSystemProperties() {
      Map<String, String> properties = new HashMap<>(super.getFileSystemProperties());
      properties.put("mapred.bq.auth.service.account.json.keyfile", serviceAccountFilePath);
      properties.put("google.cloud.auth.service.account.json.keyfile", serviceAccountFilePath);
      properties.put("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
      properties.put("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
      properties.put("fs.gs.project.id", project);
      properties.put("fs.gs.system.bucket", bucket);
      properties.put("fs.gs.impl.disable.cache", "true");
      return properties;
    }

    @Override
    protected String getPath() {
      return path;
    }
  }

}
