package io.cdap.plugin.gcp.common;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Indicates error handling strategy during record processing.
 */
public enum ErrorHandling {
  SKIP("skip-error"),
  FAIL_PIPELINE("fail-pipeline");

  private static final Map<String, ErrorHandling> BY_DISPLAY_NAME = Arrays.stream(values())
    .collect(Collectors.toMap(ErrorHandling::getDisplayName, Function.identity()));

  private final String displayName;

  ErrorHandling(String displayName) {
    this.displayName = displayName;
  }

  @Nullable
  public static ErrorHandling fromDisplayName(String displayName) {
    return BY_DISPLAY_NAME.get(displayName);
  }

  public String getDisplayName() {
    return displayName;
  }

  public static String getSupportedErrorHandling() {
    return Arrays.stream(ErrorHandling.values()).map(ErrorHandling::getDisplayName).collect(Collectors.joining(", "));
  }
}
