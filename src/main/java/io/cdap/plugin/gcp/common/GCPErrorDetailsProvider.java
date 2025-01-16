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

import com.google.api.client.http.HttpResponseException;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.cdap.etl.api.exception.ErrorDetailsProvider;

import java.util.List;

/**
 * A custom ErrorDetailsProvider for GCP plugins.
 */
public class GCPErrorDetailsProvider implements ErrorDetailsProvider {
  static final String ERROR_MESSAGE_FORMAT = "Error occurred in the phase: '%s'. %s: %s";

  /**
   * Get a ProgramFailureException with the given error
   * information from generic exceptions like {@link java.io.IOException}.
   *
   * @param e The Throwable to get the error information from.
   * @return A ProgramFailureException with the given error information, otherwise null.
   */
  public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    for (Throwable t : causalChain) {
      if (t instanceof ProgramFailureException) {
        // if causal chain already has program failure exception, return null to avoid double wrap.
        return null;
      }
      if (t instanceof HttpResponseException) {
        return GCPErrorDetailsProviderUtil.getProgramFailureException((HttpResponseException) t,
          getExternalDocumentationLink(), errorContext);
      }
      if (t instanceof IllegalArgumentException) {
        return getProgramFailureException((IllegalArgumentException) t, errorContext);
      }
      if (t instanceof IllegalStateException) {
        return getProgramFailureException((IllegalStateException) t, errorContext);
      }
    }
    return null;
  }

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link IllegalArgumentException}.
   *
   * @param e The IllegalArgumentException to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  private ProgramFailureException getProgramFailureException(IllegalArgumentException e,
      ErrorContext errorContext) {
    String errorMessage = e.getMessage();
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN),
        errorMessage, String.format(ERROR_MESSAGE_FORMAT, errorContext.getPhase(),
            e.getClass().getName(), errorMessage), ErrorType.USER, false, e);
  }

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link IllegalStateException}.
   *
   * @param e The IllegalStateException to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  private ProgramFailureException getProgramFailureException(IllegalStateException e,
      ErrorContext errorContext) {
    String errorMessage = e.getMessage();
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN),
        errorMessage, String.format(ERROR_MESSAGE_FORMAT, errorContext.getPhase(),
            e.getClass().getName(), errorMessage), ErrorType.SYSTEM, false, e);
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
