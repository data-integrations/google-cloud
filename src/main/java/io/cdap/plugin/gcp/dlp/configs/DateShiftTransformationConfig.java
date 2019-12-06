package io.cdap.plugin.gcp.dlp.configs;

import com.google.gson.Gson;
import com.google.privacy.dlp.v2.DateShiftConfig;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class DateShiftTransformationConfig implements DlpTransformConfig {

  private Integer upperBoundDays;
  private Integer lowerBoundDays;
  // private String key;
  // private String name;
  // private String wrappedKey;
  // private String cryptoKeyName;
  // private String context;
  // private CryptoKeyHelper.KeyType keyType;


  private final Schema.Type[] supportedTypes = new Schema.Type[]{Schema.Type.INT};

  @Override
  public PrimitiveTransformation toPrimitiveTransform() {

    // CryptoKey key = CryptoKeyHelper.createKey(keyType, name, this.key, cryptoKeyName, wrappedKey);
    DateShiftConfig dateShiftConfig = DateShiftConfig.newBuilder()
                                                     // .setContext(FieldId.newBuilder().setName(context).build())
                                                     // .setCryptoKey(key)
                                                     .setLowerBoundDays(lowerBoundDays)
                                                     .setUpperBoundDays(upperBoundDays)
                                                     .build();

    return PrimitiveTransformation.newBuilder().setDateShiftConfig(dateShiftConfig).build();
  }

  @Override
  public void validate(FailureCollector collector, String widgetName, ErrorConfig errorConfig) {
    // CryptoKeyHelper.validateKey(collector, widgetName, errorConfig, keyType, name, key, cryptoKeyName, wrappedKey);

    Gson gson = new Gson();
    if (upperBoundDays == null) {
      errorConfig.setNestedTransformPropertyId("upperBoundDays");
      collector.addFailure("Upper Bound is a required field for this transform.", "Please provide a value.")
               .withConfigElement(widgetName, gson.toJson(errorConfig));
    } else {
      if (Math.abs(upperBoundDays) > 365250) {
        errorConfig.setNestedTransformPropertyId("upperBoundDays");
        collector.addFailure("Upper Bound cannot be more than 10 years (365250 days) in either direction.",
                             "Please provide that is within this range.")
                 .withConfigElement(widgetName, gson.toJson(errorConfig));
      }
    }

    if (lowerBoundDays == null) {
      errorConfig.setNestedTransformPropertyId("lowerBoundDays");
      collector.addFailure("Lower Bound is a required field for this transform.", "Please provide a value.")
               .withConfigElement(widgetName, gson.toJson(errorConfig));
    } else {
      if (Math.abs(lowerBoundDays) > 365250) {
        errorConfig.setNestedTransformPropertyId("upperBoundDays");
        collector.addFailure("Lower Bound cannot be more than 10 years (365250 days) in either direction.",
                             "Please provide that is within this range.")
                 .withConfigElement(widgetName, gson.toJson(errorConfig));
      }
    }

    if (lowerBoundDays != null && upperBoundDays != null && lowerBoundDays > upperBoundDays) {
      errorConfig.setNestedTransformPropertyId("lowerBoundDays");
      collector.addFailure("Lower Bound cannot be greater than Upper Bound.", "")
               .withConfigElement(widgetName, gson.toJson(errorConfig));
    }

  }

  @Override
  public List<Schema.Type> getSupportedTypes() {
    return Arrays.asList(supportedTypes);
  }
}

/////// Json widget config for removed options ///////
// {
//   "widget-type": "input-field-selector",
//   "label": "Context",
//   "name": "context",
//   "widget-attributes": {
//   "description":"Context to be used with CryptoKey to compute date shift"
//   }
//   },
//   {
//   "name": "keyType",
//   "widget-type": "select",
//   "label": "Crypto Key Type",
//   "widget-attributes": {
//   "description": "Type of key to use for the cryptographic hash.",
//   "default": "TRANSIENT",
//   "values": [
//   {
//   "value": "TRANSIENT",
//   "label": "Transient"
//   },
//   {
//   "value": "UNWRAPPED",
//   "label": "Unwrapped Key"
//   },
//   {
//   "value": "KMS_WRAPPED",
//   "label": "KMS Wrapped Key"
//   }
//   ]
//   }
//   },
//   {
//   "name": "name",
//   "widget-type": "textbox",
//   "label": "Transient Key Name",
//   "widget-attributes": {
//   "macro": "true",
//   "placeholder": "Transient key name"
//   }
//   },
//   {
//   "name": "key",
//   "widget-type": "textbox",
//   "label": "Unwrapped Key",
//   "widget-attributes": {
//   "macro": "true",
//   "placeholder": "Unwrapped key",
//   "description": "Key to be used for cryptographic hash"
//   }
//   },
//   {
//   "name": "wrappedKey",
//   "widget-type": "textbox",
//   "label": "Wrapped Key",
//   "widget-attributes": {
//   "macro": "true",
//   "placeholder": "Wrapped key",
//   "description": "Wrapped key to be unwrapped using KMS key"
//   }
//   },
//   {
//   "name": "cryptoKeyName",
//   "widget-type": "textbox",
//   "label": "KMS Key Name",
//   "widget-attributes": {
//   "macro": "true",
//   "placeholder": "Key Name",
//   "description": "Name of the key stored in Key Management Service that will be used to unwrap the wrapped key"
//   }
//   }
