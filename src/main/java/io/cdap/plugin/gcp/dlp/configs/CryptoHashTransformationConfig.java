package io.cdap.plugin.gcp.dlp.configs;

import com.google.privacy.dlp.v2.CryptoHashConfig;
import com.google.privacy.dlp.v2.CryptoKey;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class CryptoHashTransformationConfig implements DlpTransformConfig {

  private String key;
  private String name;
  private String wrappedKey;
  private String cryptoKeyName;
  private CryptoKeyHelper.KeyType keyType;
  private final Schema.Type[] supportedTypes = new Schema.Type[]{Schema.Type.STRING};

  @Override
  public PrimitiveTransformation toPrimitiveTransform() {

    CryptoKey cryptoKey = CryptoKeyHelper.createKey(keyType, name, key, cryptoKeyName, wrappedKey);
    return PrimitiveTransformation.newBuilder()
                                  .setCryptoHashConfig(
                                    CryptoHashConfig.newBuilder()
                                                    .setCryptoKey(cryptoKey)
                                                    .build())
                                  .build();
  }


  @Override
  public void validate(FailureCollector collector, String widgetName, ErrorConfig errorConfig) {
    CryptoKeyHelper.validateKey(collector, widgetName, errorConfig, keyType, name, key, cryptoKeyName, wrappedKey);
  }

  @Override
  public List<Schema.Type> getSupportedTypes() {
    return Arrays.asList(supportedTypes);
  }
}
