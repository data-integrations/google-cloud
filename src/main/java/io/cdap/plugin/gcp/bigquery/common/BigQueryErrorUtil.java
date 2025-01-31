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

package io.cdap.plugin.gcp.bigquery.common;

import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.plugin.gcp.common.GCPErrorDetailsProviderUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class to fetch more details from BigQueryException
 */
public final class BigQueryErrorUtil {

  // https://cloud.google.com/bigquery/docs/error-messages#errortable
  private static final Map<String, ErrorType> ERROR_REASON_TO_ERROR_TYPE = new HashMap<>();
  private static final Map<String, Integer> ERROR_REASON_TO_ERROR_CODE = new HashMap<>();

  static {
    // User Errors
    ERROR_REASON_TO_ERROR_TYPE.put("invalid", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("invalidQuery", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("billingTierLimitExceeded", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("resourceInUse", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("resourcesExceeded", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("badRequest", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("invalidUser", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("notFound", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("duplicate", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("accessDenied", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("billingNotEnabled", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("quotaExceeded", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("rateLimitExceeded", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("responseTooLarge", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("blocked", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("proxyAuthenticationRequired", ErrorType.USER);
    ERROR_REASON_TO_ERROR_TYPE.put("jobRateLimitExceeded", ErrorType.USER);

    // System Errors
    ERROR_REASON_TO_ERROR_TYPE.put("tableUnavailable", ErrorType.SYSTEM);
    ERROR_REASON_TO_ERROR_TYPE.put("backendError", ErrorType.SYSTEM);
    ERROR_REASON_TO_ERROR_TYPE.put("internalError", ErrorType.SYSTEM);
    ERROR_REASON_TO_ERROR_TYPE.put("notImplemented", ErrorType.SYSTEM);
    ERROR_REASON_TO_ERROR_TYPE.put("jobBackendError", ErrorType.SYSTEM);
    ERROR_REASON_TO_ERROR_TYPE.put("jobInternalError", ErrorType.SYSTEM);
    ERROR_REASON_TO_ERROR_TYPE.put("timeout", ErrorType.SYSTEM);

    // Unknown Errors
    ERROR_REASON_TO_ERROR_TYPE.put("stopped", ErrorType.UNKNOWN);
  }

  /**
   * Method to get the error type based on the error reason.
   *
   * @param errorReason the error reason to classify
   * @return the corresponding ErrorType (USER, SYSTEM, UNKNOWN)
   */
  public static ErrorType getErrorType(String errorReason) {
    if (errorReason != null && ERROR_REASON_TO_ERROR_TYPE.containsKey(errorReason)) {
      return ERROR_REASON_TO_ERROR_TYPE.get(errorReason);
    }
    return ErrorType.UNKNOWN;
  }

  /**
   * Method to get the Program Failure exception based on error reason
   *
   * @param errorMessage
   * @param errorReason
   * @param e
   * @return
   */
  public static ProgramFailureException getProgramFailureException(String errorMessage,
      String errorReason, Exception e) {
    ErrorType errorType = BigQueryErrorUtil.getErrorType(errorReason);
    return GCPErrorDetailsProviderUtil.getHttpResponseExceptionDetailsFromChain(e, errorMessage,
        errorType, true, GCPUtils.BQ_SUPPORTED_DOC_URL);
  }
}
