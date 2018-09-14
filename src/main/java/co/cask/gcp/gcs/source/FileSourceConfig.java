/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.gcp.gcs.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.hydrator.common.ReferencePluginConfig;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Abstract file source config
 */
public abstract class FileSourceConfig extends ReferencePluginConfig {
  protected static final String MAX_SPLIT_SIZE_DESCRIPTION = "Maximum split-size for each mapper in the MapReduce " +
    "Job. Defaults to 128MB.";
  protected static final String REGEX_DESCRIPTION = "Regex to filter out files in the path. It accepts regular " +
    "expression which is applied to the complete path and returns the list of files that match the specified pattern.";
  protected static final String FILESYSTEM_PROPERTIES_DESCRIPTION = "A JSON string representing a map of properties " +
    "needed for the distributed file system.";

  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  @Nullable
  @Description(FILESYSTEM_PROPERTIES_DESCRIPTION)
  @Macro
  public String fileSystemProperties;

  @Nullable
  @Description(REGEX_DESCRIPTION)
  @Macro
  public String fileRegex;

  @Nullable
  @Description(MAX_SPLIT_SIZE_DESCRIPTION)
  @Macro
  public Long maxSplitSize;

  @Nullable
  @Description("Identify if path needs to be ignored or not, for case when directory or file does not exists. If " +
    "set to true it will treat the not present folder as zero input and log a warning. Default is false.")
  public Boolean ignoreNonExistingFolders;

  @Nullable
  @Description("Boolean value to determine if files are to be read recursively from the path. Default is false.")
  public Boolean recursive;

  @Nullable
  @Description("If specified, each output record will include a field with this name that contains the file URI " +
    "that the record was read from. Requires a customized version of CombineFileInputFormat, so it cannot be used " +
    "if an inputFormatClass is given.")
  public String pathField;

  @Nullable
  @Description("If true and a pathField is specified, only the filename will be used. If false, the full " +
    "URI will be used. Defaults to false.")
  public Boolean filenameOnly;

  // TODO: remove once CDAP-11371 is fixed
  // This is only here because the UI requires a property otherwise a default schema cannot be set.
  @Nullable
  public String schema;

  public FileSourceConfig() {
    super("");
    this.maxSplitSize = 134217728L;
    this.ignoreNonExistingFolders = false;
    this.recursive = false;
    this.filenameOnly = false;
  }

  protected void validate() {
    getFileSystemProperties();
  }

  protected Map<String, String> getFileSystemProperties() {
    if (fileSystemProperties == null) {
      return new HashMap<>();
    }
    try {
      return GSON.fromJson(fileSystemProperties, MAP_STRING_STRING_TYPE);
    } catch (Exception e) {
      throw new IllegalArgumentException("Unable to parse properties: " + e.getMessage());
    }
  }

  protected abstract String getPath();
}
