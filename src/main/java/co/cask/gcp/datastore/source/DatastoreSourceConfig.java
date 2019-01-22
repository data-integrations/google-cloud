/*
 * Copyright © 2019 Cask Data, Inc.
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
package co.cask.gcp.datastore.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.gcp.common.GCPReferenceSourceConfig;
import co.cask.gcp.datastore.source.util.DatastoreSourceSchemaUtil;
import co.cask.gcp.datastore.source.util.SourceKeyType;
import co.cask.gcp.datastore.util.DatastorePropertyUtil;
import co.cask.gcp.datastore.util.DatastoreUtil;
import com.google.cloud.datastore.PathElement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.PROPERTY_ANCESTOR;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.PROPERTY_FILTERS;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.PROPERTY_KEY_ALIAS;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.PROPERTY_KEY_TYPE;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.PROPERTY_KIND;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.PROPERTY_NAMESPACE;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.PROPERTY_NUM_SPLITS;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.PROPERTY_PROJECT;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.PROPERTY_SCHEMA;
import static co.cask.gcp.datastore.source.util.DatastoreSourceConstants.PROPERTY_SERVICE_FILE_PATH;

/**
 * This class {@link DatastoreSourceConfig} provides all the configuration required for
 * configuring the {@link DatastoreSource} plugin.
 */
public class DatastoreSourceConfig extends GCPReferenceSourceConfig {

  @Name(PROPERTY_NAMESPACE)
  @Macro
  @Nullable
  @Description("A namespace partitions entities into a subset of datastore. "
    + "If not provided, [default] namespace will be used. "
    + "(Macro Enabled)")
  private String namespace;

  @Name(PROPERTY_KIND)
  @Macro
  @Description("The kind of an entity categorizes it for the purpose of Datastore queries. "
    + "Equivalent to relational database table notion. "
    + "(Macro Enabled)")
  private String kind;

  @Name(PROPERTY_ANCESTOR)
  @Macro
  @Nullable
  @Description("Ancestor identifies the common root entity in which the entities are grouped. "
    + "Should be written in Key Literal format: key(<kind>, <identifier>, <kind>, <identifier>, [...]). "
    + "Example: `key(kind_1, 'stringId', kind_2, 100)` "
    + "(Macro Enabled)")
  private String ancestor;

  @Name(PROPERTY_FILTERS)
  @Macro
  @Nullable
  @Description("List of filter property names and values pairs by which equality filter will be applied. "
    + "This is a semi-colon separated list of key-value pairs, where each pair is separated by a pipe sign `|`. "
    + "Filter properties must be present in the schema. "
    + "Allowed property types are STRING, LONG, DOUBLE, BOOLEAN and TIMESTAMP. "
    + "Property value indicated as `null` string will be treated as `is null` clause. "
    + "TIMESTAMP string should be in the RFC 3339 format without the timezone offset (always ends in Z). "
    + "Expected pattern: `yyyy-MM-dd'T'HH:mm:ssX`, example: `2011-10-02T13:12:55Z`. "
    + "(Macro Enabled)")
  private String filters;

  @Name(PROPERTY_NUM_SPLITS)
  @Macro
  @Description("Desired number of splits to split a query into multiple shards during execution. "
    + "Will be created up to desired number of splits, however less splits can be created "
    + "if desired number is unavailable. "
    + "(Macro Enabled)")
  private int numSplits;

  @Name(PROPERTY_KEY_TYPE)
  @Macro
  @Description("Key is unique identifier assigned to the entity when it is created. Property defines if key will "
    + "be included in the output, commonly is needed to perform upserts to the Cloud Datastore. "
    + "Can be one of three options: `None` - key will not be included, `Key literal` - key will be included "
    + "in Datastore key literal format including complete path with ancestors, `URL-safe key` - key "
    + "will be included in the encoded form that can be used as part of a URL. "
    + "Note, if `Key literal` or `URL-safe key` is selected, default key name (`__key__`) or its alias must be present "
    + "in the schema with non-nullable STRING type. "
    + "(Macro Enabled)")
  private String keyType;

  @Name(PROPERTY_KEY_ALIAS)
  @Macro
  @Nullable
  @Description("Allows to set user-friendly name for the key column which default name is `__key__`. "
    + "Only applicable, if `Key Type` is set to `Key literal` or `URL-safe key`. "
    + "If `Key Type` is set to `None`, property must be empty. "
    + "(Macro Enabled)")
  private String keyAlias;

  @Name(PROPERTY_SCHEMA)
  @Macro
  @Description("Schema of the data to read, can be imported or fetched by clicking the `Get Schema` button. "
    + "(Macro Enabled)")
  private String schema;

  public DatastoreSourceConfig() {
    // needed for initialization
  }

  @VisibleForTesting
  DatastoreSourceConfig(String referenceName,
                        String project,
                        String serviceFilePath,
                        @Nullable String namespace,
                        String kind,
                        @Nullable String ancestor,
                        @Nullable String filters,
                        int numSplits,
                        String keyType,
                        @Nullable String keyAlias,
                        String schema) {
    this.referenceName = referenceName;
    this.project = project;
    this.serviceFilePath = serviceFilePath;
    this.namespace = namespace;
    this.kind = kind;
    this.ancestor = ancestor;
    this.filters = filters;
    this.numSplits = numSplits;
    this.keyType = keyType;
    this.keyAlias = keyAlias;
    this.schema = schema;
  }

  public String getReferenceName() {
    return referenceName;
  }

  public Schema getSchema() {
    return DatastorePropertyUtil.parseSchema(schema);
  }

  public String getNamespace() {
    return DatastorePropertyUtil.getNamespace(namespace);
  }

  public String getKind() {
    return kind;
  }

  public List<PathElement> getAncestor() {
    return DatastorePropertyUtil.parseKeyLiteral(ancestor);
  }

  public Map<String, String> getFilters() {
    return DatastorePropertyUtil.parseKeyValuePairs(filters);
  }

  public int getNumSplits() {
    return numSplits;
  }

  public SourceKeyType getKeyType() {
    return SourceKeyType.fromValue(keyType)
      .orElseThrow(() -> new IllegalArgumentException("Unsupported key type value: " + keyType));
  }

  public boolean isIncludeKey() {
    return SourceKeyType.NONE != getKeyType();
  }

  public String getKeyAlias() {
    return DatastorePropertyUtil.getKeyAlias(keyAlias);
  }

  @Override
  public void validate() {
    super.validate();
    validateDatastoreConnection();
    validateKind();
    validateAncestor();
    validateNumSplits();

    if (containsMacro(PROPERTY_SCHEMA)) {
      return;
    }

    Schema schema = getSchema();
    validateSchema(schema);
    validateFilters(schema);
    validateKeyType(schema);
  }

  @VisibleForTesting
  void validateDatastoreConnection() {
    if (containsMacro(PROPERTY_SERVICE_FILE_PATH) || containsMacro(PROPERTY_PROJECT)) {
      return;
    }
    DatastoreUtil.getDatastore(getServiceAccountFilePath(), getProject());
    DatastoreUtil.getDatastoreV1(getServiceAccountFilePath(), getProject());
  }

  private void validateKind() {
    if (!containsMacro(PROPERTY_KIND)) {
      DatastorePropertyUtil.validateKind(kind);
    }
  }

  private void validateAncestor() {
    if (!containsMacro(PROPERTY_ANCESTOR)) {
      getAncestor();
    }
  }

  private void validateNumSplits() {
    if (containsMacro(PROPERTY_NUM_SPLITS)) {
      return;
    }

    if (numSplits < 1) {
      throw new IllegalArgumentException("Number of split must be greater than 0");
    }
  }

  private void validateSchema(Schema schema) {
    DatastoreSourceSchemaUtil.validateSchema(schema);
  }

  private void validateFilters(Schema schema) {
    if (containsMacro(PROPERTY_FILTERS)) {
      return;
    }

    List<String> missingProperties = getFilters().keySet().stream()
      .filter(k -> schema.getField(k) == null)
      .collect(Collectors.toList());

    if (!missingProperties.isEmpty()) {
      throw new IllegalArgumentException("The following properties are missing in the schema definition: "
                                           + missingProperties);
    }
  }

  private void validateKeyType(Schema schema) {
    if (containsMacro(PROPERTY_KEY_TYPE) || containsMacro(PROPERTY_KEY_ALIAS)) {
      return;
    }

    if (isIncludeKey()) {
      DatastoreSourceSchemaUtil.validateKeyAlias(schema, getKeyAlias());
    } else {
      if (!Strings.isNullOrEmpty(keyAlias)) {
        throw new IllegalArgumentException("Key alias must be empty if Include Key is set to [None]");
      }
    }
  }

  @Override
  public String toString() {
    return "DatastoreSourceConfig{" +
      "referenceName='" + referenceName + '\'' +
      ", project='" + project + '\'' +
      ", serviceFilePath='" + serviceFilePath + '\'' +
      ", namespace='" + namespace + '\'' +
      ", kind='" + kind + '\'' +
      ", ancestor='" + ancestor + '\'' +
      ", filters='" + filters + '\'' +
      ", numSplits=" + numSplits +
      ", keyType='" + keyType + '\'' +
      ", keyAlias='" + keyAlias + '\'' +
      ", schema='" + schema + '\'' +
      "} ";
  }
}
