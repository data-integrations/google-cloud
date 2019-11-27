package io.cdap.plugin.gcp.dlp.configs;

import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.RedactConfig;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class RedactTransformConfig implements DlpTransformConfig {

  private final Schema.Type[] supportedTypes = new Schema.Type[]{Schema.Type.STRING};

  @Override
  public PrimitiveTransformation toPrimitiveTransform() {
    return PrimitiveTransformation.newBuilder().setRedactConfig(RedactConfig.newBuilder().build()).build();
  }

  @Override
  public void validate(FailureCollector collector, String widgetName, ErrorConfig errorConfig) {
    return;
  }

  @Override
  public List<Schema.Type> getSupportedTypes() {
    return Arrays.asList(supportedTypes);
  }
}
