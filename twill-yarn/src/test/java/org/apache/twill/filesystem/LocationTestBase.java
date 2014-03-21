/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.filesystem;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Base class for defining {@link Location} and {@link LocationFactory} tests.
 */
public abstract class LocationTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();


  @Test
  public void testDelete() throws IOException {
    LocationFactory factory = getLocationFactory();

    Location base = factory.create("test").getTempFile(".tmp");
    Assert.assertTrue(base.mkdirs());

    Assert.assertTrue(base.append("test1").getTempFile(".tmp").createNew());
    Assert.assertTrue(base.append("test2").getTempFile(".tmp").createNew());

    Location subDir = base.append("test3");
    Assert.assertTrue(subDir.mkdirs());

    Assert.assertTrue(subDir.append("test4").getTempFile(".tmp").createNew());
    Assert.assertTrue(subDir.append("test5").getTempFile(".tmp").createNew());

    Assert.assertTrue(base.delete(true));
    Assert.assertFalse(base.exists());
  }

  @Test
  public void testHelper() {
    LocationFactory factory = LocationFactories.namespace(getLocationFactory(), "testhelper");

    Location location = factory.create("test");
    Assert.assertTrue(location.toURI().getPath().endsWith("testhelper/test"));

    location = factory.create(URI.create("test2"));
    Assert.assertTrue(location.toURI().getPath().endsWith("testhelper/test2"));
  }

  @Test
  public void testList() throws IOException {
    LocationFactory factory = getLocationFactory();

    Location dir = factory.create("dir");

    // Check and create the directory
    Assert.assertFalse(dir.isDirectory());
    Assert.assertTrue(dir.mkdirs());
    Assert.assertTrue(dir.isDirectory());

    // Should have nothing inside
    Assert.assertTrue(dir.list().isEmpty());

    // Check and create a file inside the directory
    Location file = dir.append("file");
    Assert.assertFalse(file.isDirectory());
    Assert.assertTrue(file.createNew());
    Assert.assertFalse(file.isDirectory());

    // List on file should gives empty list
    Assert.assertTrue(file.list().isEmpty());

    // List on directory should gives the file inside
    List<Location> listing = dir.list();
    Assert.assertEquals(1, listing.size());
    Assert.assertEquals(file, listing.get(0));

    // After deleting the file inside the directory, list on directory should be empty again.
    file.delete();
    Assert.assertTrue(dir.list().isEmpty());

    // List on a non-exist location would throw exception
    try {
      file.list();
      Assert.fail("List should fail on non-exist location.");
    } catch (IOException e) {
      // Expected
    }
  }

  protected abstract LocationFactory getLocationFactory();
}
