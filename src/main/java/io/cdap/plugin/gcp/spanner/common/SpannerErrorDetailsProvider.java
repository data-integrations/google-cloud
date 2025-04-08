/*
 * Copyright © 2025 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.spanner.common;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.plugin.gcp.common.GCPErrorDetailsProvider;
import io.cdap.plugin.gcp.common.GCPUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A custom ErrorDetailsProvider for Spanner.
 */
public class SpannerErrorDetailsProvider extends GCPErrorDetailsProvider {
  private static final String ERROR_MESSAGE_FORMAT = "Error occurred in the phase: '%s'. Error message: %s";

  static Map<ErrorCode, ErrorUtils.ActionErrorPair> actionErrorMap = new HashMap<>();

  static {
    actionErrorMap.put(ErrorCode.CANCELLED, ErrorUtils.getActionErrorByStatusCode(499));
    actionErrorMap.put(ErrorCode.UNKNOWN, ErrorUtils.getActionErrorByStatusCode(500));
    actionErrorMap.put(ErrorCode.INVALID_ARGUMENT, ErrorUtils.getActionErrorByStatusCode(400));
    actionErrorMap.put(ErrorCode.DEADLINE_EXCEEDED, ErrorUtils.getActionErrorByStatusCode(504));
    actionErrorMap.put(ErrorCode.NOT_FOUND, ErrorUtils.getActionErrorByStatusCode(404));
    actionErrorMap.put(ErrorCode.ALREADY_EXISTS, ErrorUtils.getActionErrorByStatusCode(409));
    actionErrorMap.put(ErrorCode.PERMISSION_DENIED, ErrorUtils.getActionErrorByStatusCode(403));
    actionErrorMap.put(ErrorCode.UNAUTHENTICATED, ErrorUtils.getActionErrorByStatusCode(401));
    actionErrorMap.put(ErrorCode.RESOURCE_EXHAUSTED, ErrorUtils.getActionErrorByStatusCode(429));
    actionErrorMap.put(ErrorCode.FAILED_PRECONDITION, ErrorUtils.getActionErrorByStatusCode(400));
    actionErrorMap.put(ErrorCode.ABORTED, ErrorUtils.getActionErrorByStatusCode(409));
    actionErrorMap.put(ErrorCode.OUT_OF_RANGE, ErrorUtils.getActionErrorByStatusCode(400));
    actionErrorMap.put(ErrorCode.UNIMPLEMENTED, ErrorUtils.getActionErrorByStatusCode(501));
    actionErrorMap.put(ErrorCode.INTERNAL, ErrorUtils.getActionErrorByStatusCode(500));
    actionErrorMap.put(ErrorCode.UNAVAILABLE, ErrorUtils.getActionErrorByStatusCode(503));
    actionErrorMap.put(ErrorCode.DATA_LOSS, ErrorUtils.getActionErrorByStatusCode(500));
  }

  @Override
  protected String getExternalDocumentationLink() {
    return GCPUtils.SPANNER_SUPPORTED_DOC_URL;
  }

  @Override
  public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
    ProgramFailureException ex = super.getExceptionDetails(e, errorContext);
    if (ex != null) {
      return ex;
    }
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    for (Throwable t : causalChain) {
      if (t instanceof SpannerException) {
        return getProgramFailureExceptionFromSpannerException((SpannerException) t);
      }
    }
    return null;
  }

  private ProgramFailureException getProgramFailureExceptionFromSpannerException(SpannerException se) {
    String errorCodeName = se.getErrorCode().name();
    ErrorUtils.ActionErrorPair actionErrorPair = null;
    String errorReason = se.getReason();
    String errorMessage = se.getMessage();
    if (actionErrorMap.containsKey(se.getErrorCode())) {
      actionErrorPair = actionErrorMap.get(se.getErrorCode());
      errorReason = String.format("%s %s. %s", errorCodeName, errorMessage, actionErrorPair.getCorrectiveAction());
    }
    if (!errorReason.endsWith(".")) {
      errorReason = errorReason + ".";
    }
    errorReason = String.format("%s For more details, see %s.", errorReason, GCPUtils.SPANNER_SUPPORTED_DOC_URL);

    String errorMessageWithCode = String.format("[ErrorCode='%s'] %s", errorCodeName, errorMessage);
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
    errorReason, String.format("%s: %s", se.getClass().getName(), errorMessageWithCode),
    actionErrorPair != null ? actionErrorPair.getErrorType() : ErrorType.UNKNOWN, true, ErrorCodeType.HTTP,
    errorCodeName, GCPUtils.SPANNER_SUPPORTED_DOC_URL, se);
  }
}
