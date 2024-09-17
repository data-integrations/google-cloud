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
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCategory.ErrorCategoryEnum;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import java.io.IOException;

/**
 * Utility class to handle exceptions.
 */
public class ExceptionUtils {

  /** Functional interfaces for lambda-friendly method invocations */
  @FunctionalInterface
  public interface IOOperation {
    void execute() throws IOException;
  }

  /**
   * Functional interfaces for lambda-friendly method invocations.
   *
   * @param <T> the return type of the function
   */
  @FunctionalInterface
  public interface IOFunction<T> {
    T execute() throws IOException;
  }

  /** Functional interfaces for lambda-friendly method invocations */
  @FunctionalInterface
  public interface IOInterruptibleOperation {
    void execute() throws IOException, InterruptedException;
  }

  /**
   * Functional interfaces for lambda-friendly method invocations.
   *
   * @param <T> the return type of the function
   */
  @FunctionalInterface
  public interface IOInterruptibleFunction<T> {

    T execute () throws IOException, InterruptedException;
  }

  // Generic helper method to handle IOException propagation
  public static void invokeWithProgramFailureHandling(IOOperation operation) throws IOException {
    try {
      operation.execute();
    } catch (IOException e) {
      ProgramFailureException exception = getProgramFailureException(e);
      if (exception != null) {
        throw exception;
      }
      throw e;
    }
  }

  // Helper method for returning values (for methods like {@link OutputCommitter#needsTaskCommit})
  public static <T> T invokeWithProgramFailureHandling(IOFunction<T> function) throws IOException {
    try {
      return function.execute();
    } catch (IOException e) {
      ProgramFailureException exception = getProgramFailureException(e);
      if (exception != null) {
        throw exception;
      }
      throw e;
    }
  }

  // Helper method for handling both IOException and InterruptedException
  public static void invokeWithProgramFailureAndInterruptionHandling(
    IOInterruptibleOperation operation) throws IOException, InterruptedException {
    try {
      operation.execute();
    } catch (IOException e) {
      ProgramFailureException exception = getProgramFailureException(e);
      if (exception != null) {
        throw exception;
      }
      throw e;
    }
  }

  // Helper method for handling both IOException and InterruptedException
  public static <T> T invokeWithProgramFailureAndInterruptionHandling(
    IOInterruptibleFunction<T> function) throws IOException, InterruptedException {
    try {
      return function.execute();
    } catch (IOException e) {
      ProgramFailureException exception = getProgramFailureException(e);
      if (exception != null) {
        throw exception;
      }
      throw e;
    }
  }

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
   * information from {@link IOException}.
   *
   * @param e The IOException to get the error information from.
   * @return A ProgramFailureException with the given error information, otherwise null.
   */
  private static ProgramFailureException getProgramFailureException(IOException e) {
    Throwable target = e instanceof HttpResponseException ? e : e.getCause();
    if (target instanceof HttpResponseException) {
      return getProgramFailureException((HttpResponseException) target);
    }
    return null;
  }
}
