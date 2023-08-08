package io.cdap.plugin.gcp.publisher.source;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.InternalException;
import com.google.api.gax.rpc.NotFoundException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.function.Supplier;

/**
 * Tests for {@link PubSubSubscriberUtil}
 */
public class PubSubSubscriberUtilTest {
  @Test(expected = NotFoundException.class)
  public void testCallWithRetryNonRetryable() throws Exception {
    PubSubSubscriberUtil.callWithRetry(() -> {
      throw new NotFoundException(new RuntimeException("subscription not found"),
                                  GrpcStatusCode.of(Status.Code.NOT_FOUND), false);
    }, BackoffConfig.defaultInstance(), 3);
  }

  @Test(expected = RuntimeException.class)
  public void testCallWithRetryMaxRetry() throws Exception {
    PubSubSubscriberUtil.callWithRetry(() -> {
      throw new InternalException(new StatusRuntimeException(Status.INTERNAL),
                                  GrpcStatusCode.of(Status.Code.INTERNAL), false);
    }, BackoffConfig.defaultInstance(), 3);
  }

  @Test
  public void testCallWithRetrySuccess() throws Exception {
    InternalException internalException = new InternalException(new StatusRuntimeException(Status.INTERNAL),
                                                                GrpcStatusCode.of(Status.Code.INTERNAL), false);
    Supplier<String> testSupplier = Mockito.mock(Supplier.class);
    String returnValue = "success";
    Mockito.when(testSupplier.get()).thenThrow(internalException).thenThrow(internalException)
      .thenReturn(returnValue);

    String result = PubSubSubscriberUtil.callWithRetry(testSupplier, BackoffConfig.defaultInstance(), 3);
    Assert.assertSame(returnValue, result);
  }
}
