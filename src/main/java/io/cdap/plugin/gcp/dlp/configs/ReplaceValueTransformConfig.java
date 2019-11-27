package io.cdap.plugin.gcp.dlp.configs;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.ReplaceValueConfig;
import com.google.privacy.dlp.v2.Value;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class ReplaceValueTransformConfig implements DlpTransformConfig {

  private String newValue;
  private final Schema.Type[] supportedTypes = new Schema.Type[]{Schema.Type.STRING};

  @Override
  public PrimitiveTransformation toPrimitiveTransform() {
    ReplaceValueConfig.Builder builder = ReplaceValueConfig.newBuilder();
    builder.setNewValue(Value.newBuilder().setStringValue(newValue).build());
    return PrimitiveTransformation.newBuilder().setReplaceConfig(builder).build();
  }

  @Override
  public void validate(FailureCollector collector, String widgetName, ErrorConfig errorConfig) {
    Gson gson = new Gson();
    if (Strings.isNullOrEmpty(newValue)) {
      errorConfig.setNestedTransformPropertyId("newValue");
      collector.addFailure("New Value is a required field for this transform.", "Please provide a value.")
               .withConfigElement(widgetName, gson.toJson(errorConfig));
    }
  }

  @Override
  public List<Schema.Type> getSupportedTypes() {
    return Arrays.asList(supportedTypes);
  }
}
