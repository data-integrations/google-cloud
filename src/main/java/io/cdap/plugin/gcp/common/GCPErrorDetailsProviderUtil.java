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

package io.cdap.plugin.gcp.common;


import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpResponseException;
import com.google.api.gax.rpc.ApiException;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.exception.ErrorContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Common functions for GCP error details provider related functionalities.
 */
public final class GCPErrorDetailsProviderUtil {

  // https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
  public static final Map<Integer, Integer> GCP_GRPC_ERROR_CODE_HTTP_STATUS_CODE_MAP = Collections.unmodifiableMap(
      new HashMap<Integer, Integer>() {{
        put(3, 400); // INVALID_ARGUMENT <--> HTTP 400 (Bad Request)
        put(4, 504); // DEADLINE_EXCEEDED <--> HTTP 504 (Gateway Timeout)
        put(5, 404); // NOT_FOUND <--> HTTP 404 (Not Found)
        put(6, 409); // ALREADY_EXISTS <--> HTTP 409 (Conflict)
        put(7, 403); // PERMISSION_DENIED <--> HTTP 403 (Forbidden)
        put(8, 429); // RESOURCE_EXHAUSTED <--> HTTP 429 (Too Many Requests)
        put(9, 400); // FAILED_PRECONDITION <--> HTTP 400 (Bad Request)
        put(10, 409); // ABORTED <--> HTTP 409 (Conflict)
        put(11, 400); // OUT_OF_RANGE <--> HTTP 400 (Bad Request)
        put(12, 501); // UNIMPLEMENTED <--> HTTP 501 (Not Implemented)
        put(13, 500); // INTERNAL <--> HTTP 500 (Internal Server Error)
        put(14, 503); // UNAVAILABLE <--> HTTP 503 (Service Unavailable)
        put(15, 500); // DATA_LOSS <--> HTTP 500 (Internal Server Error)
        put(16, 401); // UNAUTHENTICATED <--> HTTP 401 (Unauthorized)
      }});

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link HttpResponseException}.
   *
   * @param e The HttpResponseException to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  public static ProgramFailureException getProgramFailureException(HttpResponseException e, String externalDocUrl,
                                                                   @Nullable ErrorContext errorContext) {
    Integer statusCode = e.getStatusCode();
    ErrorUtils.ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(statusCode);
    String errorReason = String.format("%s %s. %s", e.getStatusCode(), e.getStatusMessage(),
      pair.getCorrectiveAction());
    String errorMessage = e.getMessage();
    String externalDocumentationLink = null;
    if (e instanceof GoogleJsonResponseException) {
      errorMessage = getErrorMessage((GoogleJsonResponseException) e);
      externalDocumentationLink = externalDocUrl;
      if (!Strings.isNullOrEmpty(externalDocumentationLink)) {
        if (!errorReason.endsWith(".")) {
          errorReason = errorReason + ".";
        }
        errorReason = String.format("%s For more details, see %s", errorReason, externalDocumentationLink);
      }
    }
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason,
      errorContext != null ?
        String.format(GCPErrorDetailsProvider.ERROR_MESSAGE_FORMAT, errorContext.getPhase(), e.getClass().getName(),
          errorMessage) : String.format("%s: %s", e.getClass().getName(), errorMessage), pair.getErrorType(), true,
      ErrorCodeType.HTTP, statusCode.toString(), externalDocumentationLink, e);
  }

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link ApiException}.
   *
   * @param e The ApiException to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  public static ProgramFailureException getProgramFailureException(ApiException e, String externalDocUrl,
                                                                   @Nullable ErrorContext errorContext) {
    Integer statusCode = e.getStatusCode().getCode().getHttpStatusCode();
    ErrorUtils.ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(statusCode);
    String errorReason = String.format("%s %s. %s. For more details, see %s", statusCode, e.getMessage(),
     pair.getCorrectiveAction(), externalDocUrl);
    String errorMessage = e.getMessage();
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason,
      errorContext != null ?
        String.format(GCPErrorDetailsProvider.ERROR_MESSAGE_FORMAT, errorContext.getPhase(), e.getClass().getName(),
          errorMessage) : String.format("%s: %s", e.getClass().getName(), errorMessage), pair.getErrorType(), true,
      ErrorCodeType.HTTP, statusCode.toString(), externalDocUrl, e);
  }

  public static ProgramFailureException getHttpResponseExceptionDetailsFromChain(Throwable e, String errorReason,
                                                                                 ErrorType errorType,
                                                                                 boolean dependency,
                                                                                 String externalDocUrl) {
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    // Check for ProgramFailureException (avoid unnecessary re-wrapping)
    for (Throwable t : causalChain) {
      if (t instanceof ProgramFailureException) {
        return (ProgramFailureException) t;
      }
    }
    // Reverse iterate to prioritize HttpResponseException over ApiException
    for (int i = causalChain.size() - 1; i >= 0; i--) {
      Throwable t = causalChain.get(i);
      if (t instanceof HttpResponseException) {
        return getProgramFailureException((HttpResponseException) t, externalDocUrl, null);
      }
      if (t instanceof ApiException) {
        return getProgramFailureException((ApiException) t, externalDocUrl, null);
      }
    }
    // If no HttpResponseException or ApiException found in the causal chain, return generic program failure exception
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason,
      String.format("%s %s: %s", errorReason, e.getClass().getName(), e.getMessage()), errorType, dependency, e);
  }

  private static String getErrorMessage(GoogleJsonResponseException exception) {
    if (!Strings.isNullOrEmpty(exception.getMessage())) {
      return exception.getMessage();
    }
    if (exception.getDetails() != null) {
      try {
        return exception.getDetails().toPrettyString();
      } catch (IOException e) {
        return exception.getDetails().toString();
      }
    }
    return exception.getMessage();
  }


  /**
   * Get the HTTP status code for a given gRPC error code.
   *
   * @param grpcStatusCode the int value of the gRPC error code
   */
  public static ErrorUtils.ActionErrorPair getActionErrorByGrpcStatusCode(int grpcStatusCode) {
    if (!GCP_GRPC_ERROR_CODE_HTTP_STATUS_CODE_MAP.containsKey(grpcStatusCode)) {
      return null;
    }
    return ErrorUtils.getActionErrorByStatusCode(GCP_GRPC_ERROR_CODE_HTTP_STATUS_CODE_MAP.get(grpcStatusCode));
  }

  public static ProgramFailureException getProgramFailureExceptionByGrpcStatusCode(int grpcErrorCodeValue,
      String grpcErrorReason, String grpcErrorMessage, String supportedDocUrl, Exception se) {
    int httpStatusCode = GCPErrorDetailsProviderUtil.GCP_GRPC_ERROR_CODE_HTTP_STATUS_CODE_MAP.
      getOrDefault(grpcErrorCodeValue, 500);
    ErrorUtils.ActionErrorPair actionErrorPair = GCPErrorDetailsProviderUtil.getActionErrorByGrpcStatusCode(
        grpcErrorCodeValue);
    String errorReason = grpcErrorReason;
    if (actionErrorPair != null) {
      errorReason = String.format("%s %s. %s", httpStatusCode, grpcErrorMessage, actionErrorPair.getCorrectiveAction());
    }
    if (!errorReason.endsWith(".")) {
      errorReason = errorReason + ".";
    }
    errorReason = String.format("%s For more details, see %s.", errorReason, supportedDocUrl);

    String errorMessageWithCode = String.format("[ErrorCode='%s'] %s", httpStatusCode, grpcErrorMessage);
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN), errorReason,
        String.format("%s: %s", se.getClass().getName(), errorMessageWithCode),
        actionErrorPair != null ? actionErrorPair.getErrorType() : ErrorType.UNKNOWN, true, ErrorCodeType.HTTP,
        String.valueOf(httpStatusCode), supportedDocUrl, se);
  }
}
