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

package io.cdap.plugin.gcp.publisher.source;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class PubSubMessageTest {

  public byte[] getData() {
    StringBuilder s = new StringBuilder();

    for (int i = 0; i < 100; i++) {
      s.append("This is the data of a message\uDAD3\uDEA2\uD987\uDDD1N騜\uDB9E\uDE986롗");
    }

    return s.toString().getBytes();
  }

  @Test
  public void testSerializeAndDeserializePubSubMessage() throws IOException, ClassNotFoundException {
    byte[] data = getData();
    Map<String, String> attributes = new HashMap<String, String>() {{
      put("key1", "value1");
      put("key2", "value2");
      put("key3", null);
      put(null, "valueNull");
    }};
    Instant publishTime = Instant.now();

    PubSubMessage original = new PubSubMessage("messageId", "orderingKey", "ackId", data, attributes, publishTime);

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

    objectOutputStream.writeObject(original);

    objectOutputStream.flush();
    objectOutputStream.close();
    outputStream.close();

    byte[] serialized = outputStream.toByteArray();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized);
    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);

    PubSubMessage deserialized = (PubSubMessage) objectInputStream.readObject();

    objectInputStream.close();
    inputStream.close();

    //Check properties
    Assert.assertEquals(original.getMessageId(), deserialized.getMessageId());
    Assert.assertEquals(original.getAckId(), deserialized.getAckId());
    Assert.assertEquals(original.getOrderingKey(), deserialized.getOrderingKey());
    Assert.assertEquals(original.getPublishTime(), deserialized.getPublishTime());

    //Check Attributes map
    Assert.assertEquals(original.getAttributes().size(), deserialized.getAttributes().size());
    Assert.assertTrue(deserialized.getAttributes().containsKey("key1"));
    Assert.assertEquals("value1", deserialized.getAttributes().get("key1"));
    Assert.assertTrue(deserialized.getAttributes().containsKey("key2"));
    Assert.assertEquals("value2", deserialized.getAttributes().get("key2"));
    Assert.assertTrue(deserialized.getAttributes().containsKey("key3"));
    Assert.assertNull(deserialized.getAttributes().get("key3"));
    Assert.assertTrue(deserialized.getAttributes().containsKey(null));
    Assert.assertEquals("valueNull", deserialized.getAttributes().get(null));

    //Check contents of the message
    String s1 = new String(original.getData());
    String s2 = new String(deserialized.getData());
    Assert.assertEquals(s1, s2);
    Assert.assertArrayEquals(original.getData(), deserialized.getData());
  }


  @Test
  public void testSerializeAndDeserializeEmptyPubSubMessage() throws IOException, ClassNotFoundException {
    PubSubMessage original = new PubSubMessage();

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);

    objectOutputStream.writeObject(original);

    objectOutputStream.flush();
    objectOutputStream.close();
    outputStream.close();

    byte[] serialized = outputStream.toByteArray();

    ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized);
    ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);

    PubSubMessage deserialized = (PubSubMessage) objectInputStream.readObject();

    objectInputStream.close();
    inputStream.close();

    //Check properties
    Assert.assertNull(deserialized.getMessageId());
    Assert.assertNull(deserialized.getAckId());
    Assert.assertNull(deserialized.getOrderingKey());
    Assert.assertNull(deserialized.getPublishTime());

    //Check Attributes map
    Assert.assertNull(deserialized.getAttributes());

    //Check contents of the message
    Assert.assertNull(deserialized.getData());
  }
}
