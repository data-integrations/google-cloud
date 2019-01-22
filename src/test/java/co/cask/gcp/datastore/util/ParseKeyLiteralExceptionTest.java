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
package co.cask.gcp.datastore.util;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
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
  private final Matcher<String> messageMatcher;

  public ParseKeyLiteralExceptionTest(String keyLiteral, Class<? extends Throwable> exceptionClass,
                                      Matcher<String> messageMatcher) {
    this.keyLiteral = keyLiteral;
    this.exceptionClass = exceptionClass;
    this.messageMatcher = messageMatcher;
  }

  @Test
  public void testParseKeyLiteral() {
    thrown.expect(exceptionClass);
    thrown.expectMessage(messageMatcher);

    DatastorePropertyUtil.parseKeyLiteral(keyLiteral);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> data() {
    return Stream.of(
      new Object[]{"100", IllegalArgumentException.class,
        CoreMatchers.containsString("Unsupported datastore key literal format: [100]")},
      new Object[]{"'value'", IllegalArgumentException.class,
        CoreMatchers.containsString("Unsupported datastore key literal format: ['value']")},
      new Object[]{"(A,'StringId')", IllegalArgumentException.class,
        CoreMatchers.containsString("Unsupported datastore key literal format: [(A,'StringId')]")},
      new Object[]{"keyLiteral(`a a`,)", IllegalArgumentException.class,
        CoreMatchers.containsString("Unsupported datastore key literal format: [keyLiteral(`a a`,)]")},
      new Object[]{"KEY(A,100,B,200,C)", IllegalArgumentException.class, CoreMatchers.allOf(
        CoreMatchers.containsString("Datastore key literal [KEY(A,100,B,200,C)] parsing exception"),
        CoreMatchers.containsString("Key literal must have even number of elements"))
      },
      new Object[]{"key(A,)", IllegalArgumentException.class, CoreMatchers.allOf(
        CoreMatchers.containsString("Datastore key literal [key(A,)] parsing exception"),
        CoreMatchers.containsString("Key literal must have even number of elements"))
      },
      new Object[]{"key()", IllegalArgumentException.class, CoreMatchers.allOf(
        CoreMatchers.containsString("Datastore key literal [key()] parsing exception"),
        CoreMatchers.containsString("Key literal must have even number of elements"))
      }
    ).collect(Collectors.toList());
  }
}
