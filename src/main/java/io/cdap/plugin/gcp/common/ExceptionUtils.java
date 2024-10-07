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
import com.google.common.base.Throwables;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import java.io.IOException;
import java.util.List;

/**
 * Utility class to handle exceptions.
 */
public class ExceptionUtils {

  /**
   * Get a ProgramFailureException with the given error
   * information from {@link HttpResponseException}.
   *
   * @param e The HttpResponseException to get the error information from.
   * @return A ProgramFailureException with the given error information.
   */
  private static ProgramFailureException getProgramFailureException(HttpResponseException e) {
    Integer statusCode = e.getStatusCode();
    ErrorUtils.ActionErrorPair pair = ErrorUtils.getActionErrorByStatusCode(statusCode);
    String errorReason = String.format("%s %s %s", e.getStatusCode(), e.getStatusMessage(),
      pair.getCorrectiveAction());

    String errorMessage = e.getMessage();
    if (e instanceof GoogleJsonResponseException) {
      GoogleJsonResponseException exception = (GoogleJsonResponseException) e;
      errorMessage = exception.getDetails() != null ? exception.getDetails().getMessage() :
        exception.getMessage();
    }

    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategoryEnum.PLUGIN),
      errorReason, errorMessage, pair.getErrorType(), true, e);
  }

  /**
   * Get a ProgramFailureException with the given error
   * information from generic exceptions like {@link IOException}.
   *
   * @param e The Throwable to get the error information from.
   * @return A ProgramFailureException with the given error information, otherwise null.
   */
  public static ProgramFailureException getProgramFailureException(Throwable e) {

    List<Throwable> causalChain = Throwables.getCausalChain(e);
    for (Throwable t : causalChain) {
      if (t instanceof ProgramFailureException) {
        return (ProgramFailureException) t;
      }
      if (t instanceof HttpResponseException) {
        return getProgramFailureException((HttpResponseException) t);
      }
    }

    return null;
  }
}
