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

import javax.annotation.Nullable;

/**
 * Custom Exception implementation for generic use within the plugin.
 *
 */
public class ConnectorException extends Exception {

  private static final long serialVersionUID = 1L;

  private static final boolean ENABLE_SUPPRESION = false;
  private static final boolean WRITABLE_STACK_TRACE = true;

  private final String code;

  /**
   * 
   * @param code exception code
   * @param message exception message
   */
  public ConnectorException(String code, String message) {
    this(code, message, null);
  }

  /**
   * 
   * @param code exception code
   * @param cause exception cause
   */
  public ConnectorException(String code, @Nullable Throwable cause) {
    this(code, cause != null ? cause.getMessage() : null, cause);
  }

  /**
   * 
   * @param code exception code
   * @param message exception message
   * @param cause exception cause
   */
  public ConnectorException(String code, @Nullable String message, @Nullable Throwable cause) {
    this(code, message, cause, ENABLE_SUPPRESION, WRITABLE_STACK_TRACE);
  }

  /**
   * 
   * @param code exception code
   * @param message exception message
   * @param cause exception cause
   * @param enableSuppression enableSuppression
   * @param writableStackTrace exception trace
   */
  protected ConnectorException(String code, @Nullable String message, @Nullable Throwable cause,
                               boolean enableSuppression, boolean writableStackTrace) {

    super(message, cause, enableSuppression, writableStackTrace);
    this.code = code;
  }

  /**
   * @return the code
   */
  public String getCode() {
    return code;
  }
}
