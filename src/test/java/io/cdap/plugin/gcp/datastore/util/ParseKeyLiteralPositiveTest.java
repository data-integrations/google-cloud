/*
 * Copyright © 2019 Cask Data, Inc.
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
package io.cdap.plugin.gcp.datastore.util;

import com.google.datastore.v1.Key.PathElement;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests method {@link DatastorePropertyUtil#parseKeyLiteral(String)} (String)} positive cases.
 */
@RunWith(value = Parameterized.class)
public class ParseKeyLiteralPositiveTest {

  private final String keyLiteral;
  private final List<PathElement> expectedPathElements;

  public ParseKeyLiteralPositiveTest(String keyLiteral, List<PathElement> expectedPathElements) {
    this.keyLiteral = keyLiteral;
    this.expectedPathElements = expectedPathElements;
  }

  @Test
  public void testParseKeyLiteral() {
    Assert.assertEquals(expectedPathElements, DatastorePropertyUtil.parseKeyLiteral(keyLiteral));
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Stream.of(
      new Object[]{null, Collections.emptyList()},
      new Object[]{"", Collections.emptyList()},
      new Object[]{"key(A, 100)", Collections.singletonList(PathElement.newBuilder().setKind("A").setId(100).build())},
      new Object[]{"   key    (    A   , 100    )   ", Collections.singletonList(
        PathElement.newBuilder().setKind("A").setId(100).build())},

      new Object[]{"Key(A, 'stringId')", Collections.singletonList(
        PathElement.newBuilder().setKind("A").setName("stringId").build())},


      new Object[]{"KEY(`A A A`, 'stringId')", Collections.singletonList(
        PathElement.newBuilder().setKind("A A A").setName("stringId").build())},


      new Object[]{"key(A, 'stringId', B, 100, c, 200, `D D`, 'V a l u e')", Arrays.asList(
        PathElement.newBuilder().setKind("A").setName("stringId").build(),
        PathElement.newBuilder().setKind("B").setId(100).build(),
        PathElement.newBuilder().setKind("c").setId(200).build(),
        PathElement.newBuilder().setKind("D D").setName("V a l u e").build())}
        ).collect(Collectors.toList());
  }

}
