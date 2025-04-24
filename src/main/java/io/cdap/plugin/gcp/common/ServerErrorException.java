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

/**
 * Exception indicating a server-side error (HTTP 5xx).
 * <p>
 * This exception is intended to be used when a server responds with an HTTP 5xx status code,
 * which typically indicates temporary unavailability or failure on the server's part.
 * It can be used to trigger retries in retry frameworks like Failsafe.
 */
public class ServerErrorException extends RuntimeException {
  private final int statusCode;

  /**
   * Constructs a new {@code ServerErrorException} with the given status code and message.
   *
   * @param statusCode the HTTP status code (should be in the 5xx range)
   * @param message    the detail message explaining the error
   * @param cause      the original cause of the error
   */
  public ServerErrorException(int statusCode, String message, Throwable cause) {
    super("Server error [" + statusCode + "]: " + message, cause);
    this.statusCode = statusCode;
  }

  /**
   * Returns the HTTP status code associated with this server error.
   *
   * @return the 5xx HTTP status code that triggered this exception
   */
  public int getStatusCode() {
    return statusCode;
  }
}
