package io.cdap.plugin.gcp.publisher.source;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Subscription;
import io.cdap.cdap.api.macro.Macros;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.common.GCPReferenceSourceConfig;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.storage.StorageLevel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.internal.util.reflection.FieldSetter;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SubscriptionAdminClient.class)
public class PubSubReceiverSubscriptionTest {

  //Global properties used for testing.
  Credentials credentials = null;
  boolean autoAcknowledge = true;
  StorageLevel level = StorageLevels.MEMORY_ONLY;
  AtomicInteger bucket = new AtomicInteger();
  AtomicBoolean isStopped = new AtomicBoolean(false);

  //Mocks used to configure tests
  @Mock
  StatusCode statusCode;
  @Mock
  ApiException apiException;
  @Mock
  SubscriptionAdminClient subscriptionAdminClient;
  @Mock
  BackoffConfig backoffConfig;
  @Mock
  ScheduledThreadPoolExecutor executor;
  @Mock
  SubscriberStub subscriber;

  PubSubReceiver receiver;

  @Before
  public void setUp() {
    receiver = new PubSubReceiver("my-project", "my-topic", "my-subscription", credentials,
                                  autoAcknowledge, level, backoffConfig, executor, subscriber, bucket);
    receiver = spy(receiver);

    //Set up invocations to the stop method.
    isStopped.set(false);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        return isStopped.get();
      }
    }).when(receiver).isStopped();
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        isStopped.set(true);
        return null;
      }
    }).when(receiver).stop(any(), any());

    //Set up backoff config.
    when(backoffConfig.getInitialBackoffMs()).thenReturn(100);
    when(backoffConfig.getBackoffFactor()).thenReturn(2.0);
    when(backoffConfig.getMaximumBackoffMs()).thenReturn(10000);
  }

  @Test
  public void testPubSubSubscriptionMacro() throws Exception {
    PubSubSubscriberConfig config = new PubSubSubscriberConfig();

    Set<String> macroFields = new HashSet<>();
    macroFields.add(PubSubSubscriberConfig.NAME_SUBSCRIPTION);
    Set<String> lookupProperties = new HashSet<>();
    lookupProperties.add("subscription");
    Map<String, String> properties = new HashMap<>();
    properties.put(PubSubSubscriberConfig.NAME_SUBSCRIPTION, "${subscription}");
    Macros macros = new Macros(lookupProperties, null);

    PluginProperties rawProperties = PluginProperties.builder()
      .addAll(properties)
      .build()
      .setMacros(macros);

    FieldSetter.setField(config, GCPReferenceSourceConfig.class.getDeclaredField("referenceName"),
                         "pubsub_source");
    FieldSetter.setField(config, PluginConfig.class.getDeclaredField("rawProperties"), rawProperties);
    FieldSetter.setField(config, PluginConfig.class.getDeclaredField("macroFields"), macroFields);

    MockFailureCollector collector = new MockFailureCollector();
    config.validate(collector);

    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testCreateSubscriptionSuccessCase() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doReturn(subscriptionAdminClient).when(receiver).buildSubscriptionAdminClient();

    receiver.createSubscription();

    verify(subscriptionAdminClient, times(1))
      .createSubscription(any(String.class),
                          any(String.class),
                          any(PushConfig.class),
                          eq(60));
  }

  @Test
  public void testCreateSubscriptionAlreadyExistsCase() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doReturn(subscriptionAdminClient).when(receiver).buildSubscriptionAdminClient();
    when(apiException.getStatusCode()).thenReturn(statusCode);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.ALREADY_EXISTS);

    doThrow(apiException)
      .when(subscriptionAdminClient)
      .createSubscription(any(String.class),
                          any(String.class),
                          any(PushConfig.class),
                          anyInt());

    receiver.createSubscription();

    verify(subscriptionAdminClient, times(1))
      .createSubscription(any(String.class),
                          any(String.class),
                          any(PushConfig.class),
                          eq(60));
    verify(apiException, times(1))
      .getStatusCode();
  }

  @Test
  public void testCreateSubscriptionTopicDoesNotExist() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doReturn(subscriptionAdminClient).when(receiver).buildSubscriptionAdminClient();
    when(apiException.getStatusCode()).thenReturn(statusCode);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);


    doAnswer(new Answer<Void>() {
      int times = 0;

      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        times++;

        if (times != 4) {
          throw apiException;
        }

        return null;
      }
    }).when(receiver).fetchAndAck();

    doThrow(apiException)
      .when(subscriptionAdminClient)
      .createSubscription(any(String.class),
                          any(String.class),
                          any(PushConfig.class),
                          anyInt());

    receiver.createSubscription();

    verify(receiver, times(1)).stop(any(), any());
  }

  @Test
  public void testCreateSubscriptionAnyOtherAPIException() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doReturn(subscriptionAdminClient).when(receiver).buildSubscriptionAdminClient();
    // We need to throw a proper instance for ApiException and not a mock in this case.
    ApiException apiException = new ApiException(new RuntimeException(), statusCode, false);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.ABORTED);

    doThrow(apiException)
      .when(subscriptionAdminClient)
      .createSubscription(any(String.class),
                          any(String.class),
                          any(PushConfig.class),
                          anyInt());

    receiver.createSubscription();

    verify(receiver, times(1)).stop(any(), any());
  }

  public void testCreateSubscriptionWrapIOExceptionInRuntimeException() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doThrow(new IOException()).when(receiver).buildSubscriptionAdminClient();

    receiver.createSubscription();

    verify(receiver, times(1)).stop(any(), any());
  }

  @Test
  public void testSubscriptionRetryLogic() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doReturn(subscriptionAdminClient).when(receiver).buildSubscriptionAdminClient();
    when(apiException.getStatusCode()).thenReturn(statusCode);
    doReturn(true).when(receiver).isApiExceptionRetryable(apiException);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.ABORTED);

    when(subscriptionAdminClient.createSubscription(any(String.class),
                                                    any(String.class),
                                                    any(PushConfig.class),
                                                    anyInt()))
      .thenThrow(apiException)
      .thenThrow(apiException)
      .thenReturn(Subscription.newBuilder().build());

    receiver.createSubscription();

    verify(subscriptionAdminClient, times(3))
      .createSubscription(any(String.class),
                          any(String.class),
                          any(PushConfig.class),
                          eq(60));
    verify(receiver, times(2))
      .isApiExceptionRetryable(apiException);
  }

  @Test
  public void testSubscriptionRetryLogicExceedsAttempts() throws IOException {
    doCallRealMethod().when(receiver).createSubscription();
    doReturn(subscriptionAdminClient).when(receiver).buildSubscriptionAdminClient();
    when(apiException.getStatusCode()).thenReturn(statusCode);
    doReturn(true).when(receiver).isApiExceptionRetryable(apiException);
    when(statusCode.getCode()).thenReturn(StatusCode.Code.ABORTED);

    when(subscriptionAdminClient.createSubscription(any(String.class),
                                                    any(String.class),
                                                    any(PushConfig.class),
                                                    anyInt()))
      .thenThrow(apiException);

    receiver.createSubscription();

    verify(subscriptionAdminClient, times(5))
      .createSubscription(any(String.class),
                          any(String.class),
                          any(PushConfig.class),
                          eq(60));

    verify(receiver, times(1)).stop(any(), any());
  }

  @Test
  public void testSubscriptionIsStopped() {
    when(receiver.isStopped()).thenReturn(true);

    receiver.createSubscription();

    verify(subscriptionAdminClient, times(0))
      .createSubscription(any(String.class),
                          any(String.class),
                          any(PushConfig.class),
                          eq(60));
  }
}
