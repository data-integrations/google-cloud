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

package io.cdap.plugin.gcp.spanner.common;

import com.google.cloud.spanner.SpannerException;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.plugin.gcp.common.GCPErrorDetailsProvider;
import io.cdap.plugin.gcp.common.GCPErrorDetailsProviderUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import java.util.List;

/**
 * A custom ErrorDetailsProvider for Spanner.
 */
public class SpannerErrorDetailsProvider extends GCPErrorDetailsProvider {

  @Override
  protected String getExternalDocumentationLink() {
    return GCPUtils.SPANNER_SUPPORTED_DOC_URL;
  }

  @Override
  public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
    ProgramFailureException ex = super.getExceptionDetails(e, errorContext);
    if (ex != null) {
      return ex;
    }
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    for (Throwable t : causalChain) {
      if (t instanceof SpannerException) {
        return getProgramFailureExceptionFromSpannerException((SpannerException) t);
      }
    }
    return null;
  }

  private ProgramFailureException getProgramFailureExceptionFromSpannerException(SpannerException se) {
    return GCPErrorDetailsProviderUtil.getProgramFailureExceptionByGrpcStatusCode(
        se.getErrorCode().getGrpcStatusCode().value(), se.getReason(), se.getMessage(),
        GCPUtils.SPANNER_SUPPORTED_DOC_URL, se);
  }
}
