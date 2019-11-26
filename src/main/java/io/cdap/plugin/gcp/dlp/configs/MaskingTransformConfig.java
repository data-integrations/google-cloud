package io.cdap.plugin.gcp.dlp.configs;


import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.privacy.dlp.v2.CharacterMaskConfig;
import com.google.privacy.dlp.v2.CharsToIgnore;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class MaskingTransformConfig implements DlpTransformConfig {

  private String maskingChar;
  private boolean reverseOrder = false;
  private int numberToMask = 0;
  private String charsToIgnoreEnum = "COMMON_CHARS_TO_IGNORE_UNSPECIFIED";
  private final Schema.Type[] supportedTypes = new Schema.Type[]{Schema.Type.STRING};

  @Override
  public PrimitiveTransformation toPrimitiveTransform() {
    CharacterMaskConfig.Builder characterMaskConfigBuilder =
      CharacterMaskConfig.newBuilder()
                         .setMaskingCharacter(maskingChar)
                         .setReverseOrder(reverseOrder)
                         .addCharactersToIgnore(
                           CharsToIgnore.newBuilder()
                                        .setCommonCharactersToIgnore(
                                          CharsToIgnore.CommonCharsToIgnore
                                            .valueOf(
                                              charsToIgnoreEnum)
                                        )
                         );

    if (numberToMask > 0) {
      characterMaskConfigBuilder = characterMaskConfigBuilder.setNumberToMask(numberToMask);
    }

    return PrimitiveTransformation.newBuilder().setCharacterMaskConfig(characterMaskConfigBuilder).build();
  }

  @Override
  public void validate(FailureCollector collector, String widgetName, ErrorConfig errorConfig) {
    Gson gson = new Gson();
    if (Strings.isNullOrEmpty(maskingChar)) {
      errorConfig.setNestedTransformPropertyId("maskingChar");
      collector.addFailure("Masking Character is a required field for this transform.", "Please provide a value.")
               .withConfigElement(widgetName, gson.toJson(errorConfig));
    } else if (maskingChar.length() != 1) {
      errorConfig.setNestedTransformPropertyId("maskingChar");
      collector.addFailure(String.format(
        "Masking Character must be a single character, string '%s' of length %d is invalid.", maskingChar,
        maskingChar.length()), "")
               .withConfigElement(widgetName, gson.toJson(errorConfig));
    }

    if (numberToMask < 0) {
      errorConfig.setNestedTransformPropertyId("numberToMask");
      collector.addFailure("Number to mask must be a positive number", "")
               .withConfigElement(widgetName, gson.toJson(errorConfig));
    }
  }

  @Override
  public List<Schema.Type> getSupportedTypes() {
    return Arrays.asList(this.supportedTypes);
  }
}
