/*
 * Copyright © 2024 Cask Data, Inc.
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
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProvider;

import java.util.List;

/**
 * A custom ErrorDetailsProvider for GCP plugins.
 */
public class GCPErrorDetailsProvider implements ErrorDetailsProvider {

  /**
   * Get a ProgramFailureException with the given error
   * information from generic exceptions like {@link java.io.IOException}.
   *
   * @param e The Throwable to get the error information from.
   * @return A ProgramFailureException with the given error information, otherwise null.
   */
  @Override
  public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    for (Throwable t : causalChain) {
      if (t instanceof ProgramFailureException) {
        // if causal chain already has program failure exception, return null to avoid double wrap.
        return null;
      }
      if (t instanceof HttpResponseException) {
        return getProgramFailureException((HttpResponseException) t, errorContext);
      }
    }
    return null;
  }

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link HttpResponseException}.
   *
   * @param e The HttpResponseException to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  private ProgramFailureException getProgramFailureException(HttpResponseException e,
    ErrorContext errorContext) {
    Integer statusCode = e.getStatusCode();
    ErrorUtils.ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(statusCode);
    String errorReason = String.format("%s %s %s", e.getStatusCode(), e.getStatusMessage(),
      pair.getCorrectiveAction());
    String errorMessageFormat = "Error occurred in the phase: '%s'. Error message: %s";

    String errorMessage = e.getMessage();
    if (e instanceof GoogleJsonResponseException) {
      GoogleJsonResponseException exception = (GoogleJsonResponseException) e;
      errorMessage = exception.getDetails() != null ? exception.getDetails().getMessage() :
        exception.getMessage();

      String externalDocumentationLink = getExternalDocumentationLink();
      if (!Strings.isNullOrEmpty(externalDocumentationLink)) {

        if (!errorReason.endsWith(".")) {
          errorReason = errorReason + ".";
        }
        errorReason = String.format("%s For more details, see %s", errorReason,
          externalDocumentationLink);
      }
    }

    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN),
      errorReason, String.format(errorMessageFormat, errorContext.getPhase(), errorMessage),
      pair.getErrorType(), true, e);
  }

  /**
   * Get the external documentation link for the client errors if available.
   *
   * @return The external documentation link as a {@link String}.
   */
  protected String getExternalDocumentationLink() {
    return null;
  }
}
