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

package io.cdap.plugin.gcp.datastore.common;

import com.google.common.base.Throwables;
import com.google.datastore.v1.client.DatastoreException;
import com.google.rpc.Code;
import io.cdap.cdap.api.exception.ErrorCategory;
import io.cdap.cdap.api.exception.ErrorCodeType;
import io.cdap.cdap.api.exception.ErrorType;
import io.cdap.cdap.api.exception.ErrorUtils;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.plugin.gcp.common.GCPErrorDetailsProvider;
import io.cdap.plugin.gcp.common.GCPUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A custom ErrorDetailsProvider for Datastore plugins.
 */
public class DatastoreErrorDetailsProvider extends GCPErrorDetailsProvider {

  static Map<Code, ErrorUtils.ActionErrorPair> actionErrorMap = new HashMap<>();

  static {
    actionErrorMap.put(Code.CANCELLED, ErrorUtils.getActionErrorByStatusCode(499));
    actionErrorMap.put(Code.UNKNOWN, ErrorUtils.getActionErrorByStatusCode(500));
    actionErrorMap.put(Code.INVALID_ARGUMENT, ErrorUtils.getActionErrorByStatusCode(400));
    actionErrorMap.put(Code.DEADLINE_EXCEEDED, ErrorUtils.getActionErrorByStatusCode(504));
    actionErrorMap.put(Code.NOT_FOUND, ErrorUtils.getActionErrorByStatusCode(404));
    actionErrorMap.put(Code.ALREADY_EXISTS, ErrorUtils.getActionErrorByStatusCode(409));
    actionErrorMap.put(Code.PERMISSION_DENIED, ErrorUtils.getActionErrorByStatusCode(403));
    actionErrorMap.put(Code.UNAUTHENTICATED, ErrorUtils.getActionErrorByStatusCode(401));
    actionErrorMap.put(Code.RESOURCE_EXHAUSTED, ErrorUtils.getActionErrorByStatusCode(429));
    actionErrorMap.put(Code.FAILED_PRECONDITION, ErrorUtils.getActionErrorByStatusCode(400));
    actionErrorMap.put(Code.ABORTED, ErrorUtils.getActionErrorByStatusCode(409));
    actionErrorMap.put(Code.OUT_OF_RANGE, ErrorUtils.getActionErrorByStatusCode(400));
    actionErrorMap.put(Code.UNIMPLEMENTED, ErrorUtils.getActionErrorByStatusCode(501));
    actionErrorMap.put(Code.INTERNAL, ErrorUtils.getActionErrorByStatusCode(500));
    actionErrorMap.put(Code.UNAVAILABLE, ErrorUtils.getActionErrorByStatusCode(503));
    actionErrorMap.put(Code.DATA_LOSS, ErrorUtils.getActionErrorByStatusCode(500));
  }

  @Override
  protected String getExternalDocumentationLink() {
    return GCPUtils.DATASTORE_SUPPORTED_DOC_URL;
  }

  @Override
  public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
    ProgramFailureException ex = super.getExceptionDetails(e, errorContext);
    if (ex != null) {
      return ex;
    }
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    for (Throwable t : causalChain) {
      if (t instanceof DatastoreException) {
        return getProgramFailureExceptionFromDatastoreException((DatastoreException) t);
      }
    }
    return null;
  }

  private ProgramFailureException getProgramFailureExceptionFromDatastoreException(DatastoreException de) {
    String errorCodeName = de.getCode().name();
    ErrorUtils.ActionErrorPair actionErrorPair = null;
    String errorReason = de.getMessage();
    String errorMessage = de.getMessage();
    if (actionErrorMap.containsKey(de.getCode())) {
      actionErrorPair = actionErrorMap.get(de.getCode());
      errorReason = String.format("%s %s. %s", errorCodeName, errorMessage, actionErrorPair.getCorrectiveAction());
    }
    if (!errorReason.endsWith(".")) {
      errorReason = errorReason + ".";
    }
    errorReason = String.format("%s For more details, see %s", errorReason, GCPUtils.DATASTORE_SUPPORTED_DOC_URL);

    String errorMessageWithCode = String.format("[ErrorCode='%s'] %s", errorCodeName, errorMessage);
    return ErrorUtils.getProgramFailureException(new ErrorCategory(ErrorCategory.ErrorCategoryEnum.PLUGIN),
      errorReason, String.format("%s: %s", de.getClass().getName(), errorMessageWithCode),
      actionErrorPair != null ? actionErrorPair.getErrorType() : ErrorType.UNKNOWN, true, ErrorCodeType.HTTP,
      errorCodeName, GCPUtils.DATASTORE_SUPPORTED_DOC_URL, de);
  }
}
