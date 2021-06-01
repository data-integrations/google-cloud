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

package io.cdap.plugin.gcp.bigquery.connector;

import joptsimple.internal.Strings;
import org.junit.Assert;
import org.junit.Test;

public class BigQueryPathTest {

  @Test
  public void testValidPath() {
    //empty path
    BigQueryPath path = new BigQueryPath("");
    Assert.assertNull(path.getDataset());
    Assert.assertNull(path.getTable());
    Assert.assertTrue(path.isRoot());

    //root path
    path = new BigQueryPath("/");
    Assert.assertNull(path.getDataset());
    Assert.assertNull(path.getTable());
    Assert.assertTrue(path.isRoot());

    //dataset path
    path = new BigQueryPath("/dataset");
    Assert.assertEquals("dataset", path.getDataset());
    Assert.assertNull(path.getTable());
    Assert.assertFalse(path.isRoot());

    //dataset path
    path = new BigQueryPath("/dataset/");
    Assert.assertEquals("dataset", path.getDataset());
    Assert.assertNull(path.getTable());
    Assert.assertFalse(path.isRoot());

    //table path
    path = new BigQueryPath("/dataset/table");
    Assert.assertEquals("dataset", path.getDataset());
    Assert.assertEquals("table", path.getTable());
    Assert.assertFalse(path.isRoot());

    //table path
    path = new BigQueryPath("/dataset/table/");
    Assert.assertEquals("dataset", path.getDataset());
    Assert.assertEquals("table", path.getTable());
    Assert.assertFalse(path.isRoot());
  }


  @Test
  public void testInvalidPath() {
    //null path
    Assert.assertThrows("Path should not be null.", IllegalArgumentException.class, () -> new BigQueryPath(null));

    //more than two parts in the path
    Assert.assertThrows("Path should at most contain two parts.", IllegalArgumentException.class,
      () -> new BigQueryPath("/a/b/c"));

    //empty dataset
    Assert.assertThrows("Dataset should not be empty.", IllegalArgumentException.class, () -> new BigQueryPath("//"));

    //dataset exceeds max length
    Assert
      .assertThrows("Dataset is invalid, it should contain at most 1024 characters.", IllegalArgumentException.class,
        () -> new BigQueryPath("/" + Strings.repeat('a', 1025)));

    //dataset contains invalid character
    Assert.assertThrows("Dataset is invalid, it should contain only letters, numbers, and underscores.",
      IllegalArgumentException.class, () -> new BigQueryPath("/a%"));


    //empty table
    Assert
      .assertThrows("Table should not be empty.", IllegalArgumentException.class, () -> new BigQueryPath("dataset//"));

    //table exceeds max length
    Assert.assertThrows("Table is invalid, it should contain at most 1024 characters.", IllegalArgumentException.class,
      () -> new BigQueryPath("/b/" + Strings.repeat('a', 1025)));

    //table contains invalid character
    Assert.assertThrows("Dataset is invalid, it should contain only letters, numbers, and underscores.",
      IllegalArgumentException.class, () -> new BigQueryPath("/a/%"));
  }
}
