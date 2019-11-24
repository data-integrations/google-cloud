package io.cdap.plugin.gcp.dlp.configs;


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
  public void validate(FailureCollector collector) {
    // TODO Implement validation for transform properties
  }

  @Override
  public List<Schema.Type> getSupportedTypes() {
    return Arrays.asList(this.supportedTypes);
  }
}
