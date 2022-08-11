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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Tests exceptions of method {@link DatastorePropertyUtil#parseKeyLiteral(String)}.
 */
@RunWith(value = Parameterized.class)
public class ParseKeyLiteralExceptionTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private final String keyLiteral;
  private final Class<? extends Throwable> exceptionClass;

  public ParseKeyLiteralExceptionTest(String keyLiteral, Class<? extends Throwable> exceptionClass) {
    this.keyLiteral = keyLiteral;
    this.exceptionClass = exceptionClass;
  }

  @Test
  public void testParseKeyLiteral() {
    thrown.expect(exceptionClass);

    DatastorePropertyUtil.parseKeyLiteral(keyLiteral);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Stream.of(
      new Object[]{"100", IllegalArgumentException.class},
      new Object[]{"'value'", IllegalArgumentException.class},
      new Object[]{"(A,'StringId')", IllegalArgumentException.class},
      new Object[]{"keyLiteral(`a a`,)", IllegalArgumentException.class},
      new Object[]{"KEY(A,100,B,200,C)", IllegalArgumentException.class},
      new Object[]{"key(A,)", IllegalArgumentException.class},
      new Object[]{"key()", IllegalArgumentException.class}
    ).collect(Collectors.toList());
  }
}
