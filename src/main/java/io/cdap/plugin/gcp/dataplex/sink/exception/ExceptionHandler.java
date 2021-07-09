/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.plugin.gcp.dataplex.sink.exception;

/**
 * Util class for Exception handling
 *
 */
public class ExceptionHandler {

  private ExceptionHandler() {
  }

  /**
   * Iterates over the Exception hierarchy and extracts the last root cause in
   * the stack.
   * 
   * @param e Exception
   * @return Throwable, the root cause of Exceptions
   */
  private static Throwable getRootCause(Exception e) {
    Throwable cause = null;
    Throwable result = e;

    while (null != (cause = result.getCause()) && (result != cause)) {
      result = cause;
    }

    return result;
  }

  /**
   * Traverses the exception hierarchy and returns the user friendly message from
   * root cause
   * 
   * @param e Exception
   * @return Exception root cause message
   */
  public static String getRootMessage(Exception e) {
    Throwable root = getRootCause(e);
    String rootMsg = root.getMessage();
    // Contains too much technical info not suitable for display on UI
    if (rootMsg.indexOf("\n") > -1) {
      return rootMsg.substring(0, rootMsg.indexOf("\n"));
    }

    return rootMsg;
  }
}
