package io.cdap.plugin.gcp.dlp.configs;

import com.google.privacy.dlp.v2.PrimitiveTransformation;
import io.cdap.cdap.etl.api.FailureCollector;

/**
 *
 */
public interface DlpTransformConfig {

  PrimitiveTransformation toPrimitiveTransform();

  void validate(FailureCollector collector);
}
