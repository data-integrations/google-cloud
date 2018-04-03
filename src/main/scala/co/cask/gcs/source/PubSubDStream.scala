/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.gcs.source

import java.io.{File, FileInputStream}
import java.util

import co.cask.cdap.api.data.format.StructuredRecord
import co.cask.cdap.api.data.schema.Schema
import com.google.api.gax.core.FixedCredentialsProvider
import com.google.auth.Credentials
import com.google.auth.oauth2.ServiceAccountCredentials
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings
import com.google.cloud.pubsub.v1.stub.{GrpcSubscriberStub, SubscriberStub}
import com.google.pubsub.v1.{AcknowledgeRequest, PullRequest, ReceivedMessage, SubscriptionName}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{StreamingContext, Time}

import scala.collection.JavaConversions._
/**
  * Read from Google PubSub.
  */
class PubSubDStream (@transient ssc: StreamingContext,
                     projectId: String,
                     subscriberId: String,
                     serviceFilePath: String,
                     maxMessagePerPoll: Int) extends InputDStream[StructuredRecord](ssc: StreamingContext) {
  @transient val DEFAULT_SCHEMA = Schema.recordOf("event", Schema.Field.of("message",
    Schema.of(Schema.Type.STRING)), Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("timestamp", Schema.of(Schema.Type.LONG)))

  @transient lazy val subscriptionName: String = SubscriptionName.create(projectId, subscriberId).toString
  @transient lazy val subscriber: SubscriberStub = createSubscriberStub();

  override def start(): Unit = {
    // no-op
  }

  override def stop(): Unit = {
    // no-op
  }

  override def compute(validTime: Time): Option[RDD[StructuredRecord]] = {
    val pullRequest = PullRequest.newBuilder
      .setMaxMessages(maxMessagePerPoll)
      .setReturnImmediately(true)
      .setSubscription(subscriptionName).build
    val pullResponse = subscriber.pullCallable.call(pullRequest)
    val messages = pullResponse.getReceivedMessagesList
    val ackIds = new util.ArrayList[String](messages.map(_.getAckId))

    if (!ackIds.isEmpty) {


      // acknowledge received messages
      val acknowledgeRequest = AcknowledgeRequest.newBuilder.setSubscription(subscriptionName).addAllAckIds(ackIds).build
      // use acknowledgeCallable().futureCall to asynchronously perform this operation
      subscriber.acknowledgeCallable.call(acknowledgeRequest)


      val result = messages.map(messages => {
        StructuredRecord.builder(DEFAULT_SCHEMA)
          .set("message", messages.getMessage.getData.toStringUtf8)
          .set("id", messages.getMessage.getMessageId)
          .set("timestamp", messages.getMessage.getPublishTime.getSeconds)
          .build

      })

      Some(ssc.sparkContext.parallelize(result))

    } else {
      Some(ssc.sparkContext.emptyRDD)
    }
  }

  private def createSubscriberStub(): SubscriberStub = {
    GrpcSubscriberStub.create(SubscriptionAdminSettings.newBuilder().
      setCredentialsProvider(new FixedCredentialsProvider() {
        override def getCredentials: Credentials = {
          val credentialsPath = new File(serviceFilePath)
          if (!credentialsPath.exists()) {
            throw new IllegalArgumentException("credentialsPath does not exist")
          }
          ServiceAccountCredentials.fromStream(new FileInputStream(credentialsPath))
        }
      }).build())
  }
}
