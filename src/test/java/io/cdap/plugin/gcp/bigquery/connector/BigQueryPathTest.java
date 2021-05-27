/*
 *
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

package io.cdap.plugin.gcp.bigquery.connector;

import joptsimple.internal.Strings;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BigQueryPathTest {


  @Test
  public void testValidPath() {
    //empty path
    BigQueryPath path = new BigQueryPath("");
    assertNull(path.getDataset());
    assertNull(path.getTable());
    assertTrue(path.isRoot());

    //root path
    path = new BigQueryPath("/");
    assertNull(path.getDataset());
    assertNull(path.getTable());
    assertTrue(path.isRoot());

    //dataset path
    path = new BigQueryPath("/dataset");
    assertEquals("dataset", path.getDataset());
    assertNull(path.getTable());
    assertFalse(path.isRoot());

    //dataset path
    path = new BigQueryPath("/dataset/");
    assertEquals("dataset", path.getDataset());
    assertNull(path.getTable());
    assertFalse(path.isRoot());

    //table path
    path = new BigQueryPath("/dataset/table");
    assertEquals("dataset", path.getDataset());
    assertEquals("table", path.getTable());
    assertFalse(path.isRoot());

    //table path
    path = new BigQueryPath("/dataset/table/");
    assertEquals("dataset", path.getDataset());
    assertEquals("table", path.getTable());
    assertFalse(path.isRoot());
  }


  @Test
  public void testInvalidPath() {
    //null path
    assertThrows("Path should not be null.", IllegalArgumentException.class, () -> new BigQueryPath(null));

    //more than two parts in the path
    assertThrows("Path should at most contain two parts.", IllegalArgumentException.class,
      () -> new BigQueryPath("/a/b/c"));

    //empty dataset
    assertThrows("Dataset should not be empty.", IllegalArgumentException.class, () -> new BigQueryPath("//"));

    //dataset exceeds max length
    assertThrows("Dataset is invalid, it should contain at most 1024 characters.", IllegalArgumentException.class,
      () -> new BigQueryPath("/" + Strings.repeat('a', 1025)));

    //dataset contains invalid character
    assertThrows("Dataset is invalid, it should contain only letters, numbers, and underscores.",
      IllegalArgumentException.class,
      () -> new BigQueryPath("/a%"));


    //empty table
    assertThrows("Table should not be empty.", IllegalArgumentException.class, () -> new BigQueryPath("dataset//"));

    //table exceeds max length
    assertThrows("Table is invalid, it should contain at most 1024 characters.", IllegalArgumentException.class,
      () -> new BigQueryPath("/b/" + Strings.repeat('a', 1025)));

    //table contains invalid character
    assertThrows("Dataset is invalid, it should contain only letters, numbers, and underscores.",
      IllegalArgumentException.class,
      () -> new BigQueryPath("/a/%"));


  }
}
