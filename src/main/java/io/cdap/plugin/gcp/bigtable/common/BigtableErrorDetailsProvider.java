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

package io.cdap.plugin.gcp.bigtable.common;

import com.google.bigtable.repackaged.io.grpc.StatusRuntimeException;
import com.google.common.base.Throwables;
import io.cdap.cdap.api.exception.ProgramFailureException;
import io.cdap.cdap.etl.api.exception.ErrorContext;
import io.cdap.plugin.gcp.common.GCPErrorDetailsProvider;
import io.cdap.plugin.gcp.common.GCPErrorDetailsProviderUtil;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import java.util.List;


/**
 * A custom ErrorDetailsProvider for BigTable plugins.
 */
public class BigtableErrorDetailsProvider extends GCPErrorDetailsProvider {

  @Override
  protected String getExternalDocumentationLink() {
    return GCPUtils.BIG_TABLE_SUPPORTED_DOC_URL;
  }

  @Override
  public ProgramFailureException getExceptionDetails(Exception e, ErrorContext errorContext) {
    ProgramFailureException ex = super.getExceptionDetails(e, errorContext);
    if (ex != null) {
      return ex;
    }
    List<Throwable> causalChain = Throwables.getCausalChain(e);
    for (Throwable t : causalChain) {
      if (t instanceof StatusRuntimeException) {
        return getProgramFailureExceptionFromBigTableException((StatusRuntimeException) t);
      }
      // Some RPC exception may be wrapped in a RetriesExhaustedWithDetailsException
      if (t instanceof RetriesExhaustedWithDetailsException) {
        RetriesExhaustedWithDetailsException r = (RetriesExhaustedWithDetailsException) t;
        List<Throwable> innerCauses = r.getCauses();
        for (Throwable innerCause : innerCauses) {
          if (innerCause instanceof Exception) {
            ProgramFailureException pfe = this.getExceptionDetails((Exception) innerCause, errorContext);
            if (pfe != null) {
              return pfe;
            }
          }
        }
      }
    }
    return null;
  }

  private ProgramFailureException getProgramFailureExceptionFromBigTableException(StatusRuntimeException se) {
    return GCPErrorDetailsProviderUtil.getProgramFailureExceptionByGrpcStatusCode(se.getStatus().getCode().value(),
        se.getMessage(), se.getMessage(), GCPUtils.BIG_TABLE_SUPPORTED_DOC_URL, se);
  }
}
