package io.cdap.plugin.gcp.dlp.configs;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.privacy.dlp.v2.CryptoHashConfig;
import com.google.privacy.dlp.v2.CryptoKey;
import com.google.privacy.dlp.v2.KmsWrappedCryptoKey;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.TransientCryptoKey;
import com.google.privacy.dlp.v2.UnwrappedCryptoKey;
import com.google.protobuf.ByteString;
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

  private enum KeyType {
    TRANSIENT,
    UNWRAPPED,
    KMS_WRAPPED
  }

  private KeyType keyType;
  private final Schema.Type[] supportedTypes = new Schema.Type[]{Schema.Type.STRING};

  @Override
  public PrimitiveTransformation toPrimitiveTransform() {

    CryptoKey.Builder cryptoKeyBuilder = CryptoKey.newBuilder();
    switch (keyType) {
      case TRANSIENT:
        cryptoKeyBuilder.setTransient(
          TransientCryptoKey.newBuilder()
                            .setName(name)
                            .build());
        break;
      case UNWRAPPED:
        cryptoKeyBuilder.setUnwrapped(
          UnwrappedCryptoKey.newBuilder()
                            .setKey(ByteString.copyFromUtf8(key))
                            .build());
        break;
      case KMS_WRAPPED:
        cryptoKeyBuilder.setKmsWrapped(
          KmsWrappedCryptoKey.newBuilder()
                             .setCryptoKeyName(cryptoKeyName)
                             .setWrappedKey(ByteString.copyFromUtf8(wrappedKey))
                             .build());
        break;
    }
    return PrimitiveTransformation.newBuilder()
                                  .setCryptoHashConfig(
                                    CryptoHashConfig.newBuilder()
                                                    .setCryptoKey(cryptoKeyBuilder)
                                                    .build())
                                  .build();
  }

  @Override
  public void validate(FailureCollector collector, String widgetName, ErrorConfig errorConfig) {
    Gson gson = new Gson();
    if (keyType == null) {
      errorConfig.setNestedTransformPropertyId("keyType");
      collector.addFailure("Crypto Key Type is a required field for this transform.", "Please provide a value.")
               .withConfigElement(widgetName, gson.toJson(errorConfig));
      return;
    }
    switch (keyType) {
      case TRANSIENT:
        if (Strings.isNullOrEmpty(name)) {
          errorConfig.setNestedTransformPropertyId("name");
          collector.addFailure("Transient Key Name is a required field for this transform.", "Please provide a value.")
                   .withConfigElement(widgetName, gson.toJson(errorConfig));
        }
        break;

      case UNWRAPPED:
        if (Strings.isNullOrEmpty(key)) {
          errorConfig.setNestedTransformPropertyId("key");
          collector.addFailure("Key is a required field for this transform.", "Please provide a value.")
                   .withConfigElement(widgetName, gson.toJson(errorConfig));
        } else {
          if (!(key.length() == 16 || key.length() == 24 || key.length() == 32)) {
            errorConfig.setNestedTransformPropertyId("key");
            collector.addFailure("Key must be 128/192/256 bit.", "Please provide a key of the correct length.")
                     .withConfigElement(widgetName, gson.toJson(errorConfig));
          }
        }
        break;
      case KMS_WRAPPED:
        if (Strings.isNullOrEmpty(wrappedKey)) {
          errorConfig.setNestedTransformPropertyId("wrappedKey");
          collector.addFailure("Wrapped Key is a required field for this transform.", "Please provide a value.")
                   .withConfigElement(widgetName, gson.toJson(errorConfig));
        }

        if (Strings.isNullOrEmpty(cryptoKeyName)) {
          errorConfig.setNestedTransformPropertyId("cryptoKeyName");
          collector.addFailure("Crypto Key Name is a required field for this transform.", "Please provide a value.")
                   .withConfigElement(widgetName, gson.toJson(errorConfig));
        }
        break;
    }
  }

  @Override
  public List<Schema.Type> getSupportedTypes() {
    return Arrays.asList(supportedTypes);
  }
}
