package io.cdap.plugin.gcp.dlp.configs;

import com.google.privacy.dlp.v2.PrimitiveTransformation;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.List;

/**
 *
 */
public interface DlpTransformConfig {

  PrimitiveTransformation toPrimitiveTransform();

  void validate(FailureCollector collector, String widgetName, ErrorConfig errorConfig);

  List<Schema.Type> getSupportedTypes();
}
