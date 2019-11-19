package io.cdap.plugin.gcp.dlp.configs;


import com.google.privacy.dlp.v2.CharacterMaskConfig;
import com.google.privacy.dlp.v2.CharsToIgnore;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import io.cdap.cdap.etl.api.FailureCollector;

/**
 *
 */
public class MaskingTransformConfig implements DlpTransformConfig {

  private String maskingChar;
  private boolean reverseOrder;
  private int numberToMask;
  private String charsToIgnoreEnum;


  @Override
  public PrimitiveTransformation toPrimitiveTransform() {
    CharacterMaskConfig.Builder characterMaskConfigBuilder = CharacterMaskConfig.newBuilder()
      .setMaskingCharacter(maskingChar)
      .setReverseOrder(reverseOrder)
      .setCharactersToIgnore(0, CharsToIgnore.newBuilder().setCommonCharactersToIgnore(
        CharsToIgnore.CommonCharsToIgnore.valueOf(charsToIgnoreEnum)));

    if (numberToMask > 0) {
      characterMaskConfigBuilder = characterMaskConfigBuilder.setNumberToMask(numberToMask);
    }

    return PrimitiveTransformation.newBuilder().setCharacterMaskConfig(characterMaskConfigBuilder).build();
  }

  @Override
  public void validate(FailureCollector collector) {
    // TODO Implement validation for transform properties
  }
}
