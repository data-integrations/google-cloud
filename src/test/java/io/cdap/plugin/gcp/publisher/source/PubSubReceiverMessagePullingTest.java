package io.cdap.plugin.gcp.publisher.source;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.auth.Credentials;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.protobuf.Empty;
import com.google.protobuf.GeneratedMessageV3;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.storage.StorageLevel;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.spy;

@RunWith(PowerMockRunner.class)
@PrepareForTest({UnaryCallable.class, PullResponse.class, GeneratedMessageV3.class, ReceivedMessage.class})
public class PubSubReceiverMessagePullingTest {

  //Global properties used for testing.
  Credentials credentials = null;
  StorageLevel level = StorageLevels.MEMORY_ONLY;
  AtomicInteger tokenBucket = new AtomicInteger();
  AtomicBoolean isStopped = new AtomicBoolean(false);

  //Mocks used to configure tests
  @Mock
  StatusCode statusCode;
  @Mock
  ApiException apiException;
  @Mock
  SubscriberStubSettings subscriberSettings;
  @Mock
  SubscriberStub subscriber;
  @Mock
  UnaryCallable<PullRequest, PullResponse> pullCallable;
  @Mock
  UnaryCallable<AcknowledgeRequest, Empty> acknowledgeCallable;
  @Mock
  BackoffConfig backoffConfig;
  @Mock
  ScheduledThreadPoolExecutor executor;

  //Argument Captors
  @Captor
  ArgumentCaptor<PullRequest> pullRequestArgumentCaptor;
  @Captor
  ArgumentCaptor<AcknowledgeRequest> acknowledgeRequestArgumentCaptor;

  PubSubReceiver receiver;

  final Answer<Boolean> isReceiverStopped = invocationOnMock -> isStopped.get();

  final Answer<Void> stopReceiver = invocationOnMock -> {
    isStopped.set(true);
    return null;
  };

  @Before
  public void setup() throws IOException {
    configureReceiver(true);

    //Set up subscriber settings and subscriber instance.
    doReturn(subscriberSettings).when(receiver).buildSubscriberSettings();
    doReturn(subscriber).when(receiver).buildSubscriberClient();
    when(subscriber.pullCallable()).thenReturn(pullCallable);
    when(subscriber.acknowledgeCallable()).thenReturn(acknowledgeCallable);

    when(backoffConfig.getInitialBackoffMs()).thenReturn(100);
    when(backoffConfig.getBackoffFactor()).thenReturn(2.0);
    when(backoffConfig.getMaximumBackoffMs()).thenReturn(10000);

    tokenBucket.set(12345);
  }

  public void configureReceiver(boolean autoAcknowledge) {
    receiver = new PubSubReceiver("my-project", "my-topic", "my-subscription", credentials,
                                  autoAcknowledge, level, backoffConfig, executor, subscriber, tokenBucket);
    receiver = spy(receiver);

    //Set up receiver status
    isStopped.set(false);
    doAnswer(isReceiverStopped).when(receiver).isStopped();
    doAnswer(stopReceiver).when(receiver).stop(any(), any());
    doAnswer(stopReceiver).when(receiver).restart(any(), any());
    doNothing().when(receiver).store(any(Iterator.class));
  }

  public ReceivedMessage getReceivedMessage(String ackId) {
    ReceivedMessage message = ReceivedMessage.newBuilder().setAckId(ackId).buildPartial();
    return message;
  }

  @Test
  public void testScheduleFetchSchedulesTask() throws IOException {
    receiver.scheduleTasks();

    verify(executor, times(1))
      .scheduleAtFixedRate(any(), eq(0L), eq(1L), eq(TimeUnit.SECONDS));
    verify(executor, times(1))
      .scheduleWithFixedDelay(any(), eq(100L), eq(100L), eq(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testScheduleFetchDoesntScheduleTasksWhenReceiverIsStopped() throws IOException {
    doReturn(true).when(receiver).isStopped();

    receiver.scheduleTasks();

    verify(executor, times(0))
      .scheduleAtFixedRate(any(), anyLong(), anyLong(), any());
    verify(executor, times(0))
      .scheduleWithFixedDelay(any(), anyLong(), anyLong(), any());
  }

  @Test
  public void testReceiveMessagesSuccess() throws IOException {
    doNothing().when(receiver).fetchAndAck();
    //Stop after 3 iterations
    when(receiver.isStopped())
      .thenReturn(false);

    receiver.receiveMessages();

    verify(receiver, times(1)).fetchAndAck();
  }

  @Test
  public void testReceiveMessagesWithBackoff() throws IOException {
    doReturn(true).when(receiver).isApiExceptionRetryable(apiException);

    doThrow(apiException).when(receiver).fetchAndAck();

    //Run 5 iterations and then stop.
    doReturn(200, 400, 800, 1600, 3200).when(receiver).sleepAndIncreaseBackoff(anyInt());
    doReturn(false, false, false, false, false, true).when(receiver).isStopped();

    receiver.receiveMessages();

    verify(receiver, times(5)).fetchAndAck();
    verify(receiver, times(5)).sleepAndIncreaseBackoff(anyInt());
    verify(receiver).sleepAndIncreaseBackoff(100);
    verify(receiver).sleepAndIncreaseBackoff(200);
    verify(receiver).sleepAndIncreaseBackoff(400);
    verify(receiver).sleepAndIncreaseBackoff(800);
    verify(receiver).sleepAndIncreaseBackoff(1600);
  }

  @Test
  public void testReceiveMessagesWithBackoffRecovery() throws IOException {
    doReturn(true).when(receiver).isApiExceptionRetryable(apiException);

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

    //Run 4 iterations in total. Fail the first 3 times and then succeed.
    doReturn(200, 400, 800)
      .when(receiver).sleepAndIncreaseBackoff(anyInt());
    doReturn(false).when(receiver).isStopped();

    receiver.receiveMessages();

    verify(receiver, times(4)).fetchAndAck();
    verify(backoffConfig, times(1)).getInitialBackoffMs();
    verify(receiver, times(3)).sleepAndIncreaseBackoff(anyInt());
    verify(receiver, times(1)).sleepAndIncreaseBackoff(100);
    verify(receiver, times(1)).sleepAndIncreaseBackoff(200);
    verify(receiver, times(1)).sleepAndIncreaseBackoff(400);
  }

  @Test
  public void testReceiveMessagesRestartsReceiverOnNonRetryableApiException() throws IOException {
    doReturn(false).when(receiver).isApiExceptionRetryable(apiException);

    doThrow(apiException).when(receiver).fetchAndAck();

    receiver.receiveMessages();

    verify(receiver, times(1)).restart(any(), eq(apiException));
    verify(receiver, times(1)).isStopped();
  }

  @Test
  public void testFetchAndAckWithAutoAcknowledge() throws IOException {
    //Set up messages list
    List<ReceivedMessage> messages = Arrays.asList(getReceivedMessage("a"), getReceivedMessage("b"));
    PullResponse response = PullResponse.newBuilder().addAllReceivedMessages(messages).buildPartial();

    when(pullCallable.call(any())).thenReturn(response);

    receiver.fetchAndAck();

    verify(pullCallable, times(1)).call(pullRequestArgumentCaptor.capture());
    verify(receiver, times(1)).store(any(Iterator.class));
    verify(acknowledgeCallable, times(1)).call(acknowledgeRequestArgumentCaptor.capture());

    PullRequest pullRequest = pullRequestArgumentCaptor.getValue();
    Assert.assertEquals(pullRequest.getMaxMessages(), 12345);

    AcknowledgeRequest acknowledgeRequest = acknowledgeRequestArgumentCaptor.getValue();
    Assert.assertEquals(acknowledgeRequest.getAckIds(0), "a");
    Assert.assertEquals(acknowledgeRequest.getAckIds(1), "b");
  }

  @Test
  public void testFetchAndAckWithoutAutoAcknowledge() throws IOException {
   configureReceiver(false);

    //Set up messages list
    List<ReceivedMessage> messages = Arrays.asList(getReceivedMessage("a"), getReceivedMessage("b"));
    PullResponse response = PullResponse.newBuilder().addAllReceivedMessages(messages).buildPartial();

    when(pullCallable.call(any())).thenReturn(response);

    receiver.fetchAndAck();

    verify(pullCallable, times(1)).call(pullRequestArgumentCaptor.capture());
    verify(receiver, times(1)).store(any(Iterator.class));
    verify(acknowledgeCallable, times(0)).call(acknowledgeRequestArgumentCaptor.capture());

    PullRequest pullRequest = pullRequestArgumentCaptor.getValue();
    Assert.assertEquals(pullRequest.getMaxMessages(), 12345);
  }

  @Test
  public void testFetchAndAckReturnsNoNewMessages() throws IOException {
    //Set up messages list
    List<ReceivedMessage> messages = Collections.emptyList();
    PullResponse response = PullResponse.newBuilder().addAllReceivedMessages(messages).buildPartial();

    when(pullCallable.call(any())).thenReturn(response);

    receiver.fetchAndAck();

    verify(pullCallable, times(1)).call(pullRequestArgumentCaptor.capture());
    verify(receiver, times(0)).store(any(Iterator.class));
    verify(acknowledgeCallable, times(0)).call(acknowledgeRequestArgumentCaptor.capture());

    PullRequest pullRequest = pullRequestArgumentCaptor.getValue();
    Assert.assertEquals(pullRequest.getMaxMessages(), 12345);
  }

  @Test
  public void testMessageRateCalculation() {
    int backoff;

    when(backoffConfig.getInitialBackoffMs()).thenReturn(100);
    when(backoffConfig.getBackoffFactor()).thenReturn(2.0);
    when(backoffConfig.getMaximumBackoffMs()).thenReturn(1000);

    backoff = 100;
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 200);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 400);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 800);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 1000);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 1000);

    when(backoffConfig.getInitialBackoffMs()).thenReturn(50);
    when(backoffConfig.getBackoffFactor()).thenReturn(4.0);
    when(backoffConfig.getMaximumBackoffMs()).thenReturn(950);

    backoff = 50;
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 200);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 800);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 950);
    backoff = receiver.calculateUpdatedBackoff(backoff);
    Assert.assertEquals(backoff, 950);
  }

  @Test
  public void testIsRetryableReturnsTrueWhenAPIExceptionIsRetryable() {
    when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
    when(apiException.isRetryable()).thenReturn(true);
    when(apiException.getStatusCode()).thenReturn(statusCode);

    Assert.assertTrue(receiver.isApiExceptionRetryable(apiException));

    verify(apiException, times(1)).isRetryable();
    verify(apiException, times(0)).getStatusCode();
  }

  @Test
  public void testIsRetryableReturnsTrueWhenExceptionStatusCodeMatchesValues() {
    when(statusCode.getCode()).thenReturn(StatusCode.Code.DEADLINE_EXCEEDED);
    when(apiException.isRetryable()).thenReturn(false);
    when(apiException.getStatusCode()).thenReturn(statusCode);

    Assert.assertTrue(receiver.isApiExceptionRetryable(apiException));

    verify(apiException, times(1)).isRetryable();
    verify(apiException, times(1)).getStatusCode();
  }

  @Test
  public void testIsRetryableReturnsFalseWhenNoConditionsMatch() {
    when(statusCode.getCode()).thenReturn(StatusCode.Code.NOT_FOUND);
    when(apiException.isRetryable()).thenReturn(false);
    when(apiException.getStatusCode()).thenReturn(statusCode);

    Assert.assertFalse(receiver.isApiExceptionRetryable(apiException));

    verify(apiException, times(1)).isRetryable();
    verify(apiException, times(1)).getStatusCode();
  }

}
