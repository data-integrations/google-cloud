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
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.exception.ErrorContext;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Common functions for GCP error details provider related functionalities.
 */
public final class GCPErrorDetailsProviderUtil {

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

  public static ProgramFailureException getHttpResponseExceptionDetailsFromChain(Exception e, String errorReason,
                                                                                 ErrorType errorType,
                                                                                 boolean dependency,
                                                                                 String externalDocUrl) {
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    for (Throwable t : causalChain) {
      if (t instanceof ProgramFailureException) {
        // Avoid double wrap
        return (ProgramFailureException) t;
      }
      if (t instanceof HttpResponseException) {
        return getProgramFailureException((HttpResponseException) t, externalDocUrl, null);
      }
    }
    // If no HttpResponseException found in the causal chain, return generic program failure exception
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
}
