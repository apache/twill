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

import com.google.common.base.Charsets;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.io.CharStreams;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * Base class for defining {@link Location} and {@link LocationFactory} tests.
 */
public abstract class LocationTestBase {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  private final LoadingCache<String, LocationFactory> locationFactoryCache = CacheBuilder.newBuilder()
    .build(new CacheLoader<String, LocationFactory>() {
      @Override
      public LocationFactory load(String key) throws Exception {
        return createLocationFactory(key);
      }
    });

  @Test
  public void testBasic() throws Exception {
    LocationFactory factory = locationFactoryCache.getUnchecked("basic");
    URI baseURI = factory.create("/").toURI();

    // Test basic location construction
    Assert.assertEquals(factory.create("/file"), factory.create("/file"));
    Assert.assertEquals(factory.create("/file2"),
                        factory.create(URI.create(baseURI.getScheme() + ":" + baseURI.getPath() + "/file2")));
    Assert.assertEquals(factory.create("/file3"),
                        factory.create(
                          new URI(baseURI.getScheme(), baseURI.getAuthority(),
                                  baseURI.getPath() + "/file3", null, null)));
    Assert.assertEquals(factory.create("/"), factory.create("/"));
    Assert.assertEquals(factory.create("/"), factory.create(URI.create(baseURI.getScheme() + ":" + baseURI.getPath())));

    Assert.assertEquals(factory.create("/"),
                        factory.create(new URI(baseURI.getScheme(), baseURI.getAuthority(),
                                               baseURI.getPath(), null, null)));

    // Test file creation and rename
    Location location = factory.create("/file");
    Assert.assertTrue(location.createNew());
    Assert.assertTrue(location.exists());

    Location location2 = factory.create("/file2");
    String message = "Testing Message";
    try (Writer writer = new OutputStreamWriter(location2.getOutputStream(), Charsets.UTF_8)) {
      writer.write(message);
    }
    long length = location2.length();
    long lastModified = location2.lastModified();

    location2.renameTo(location);

    Assert.assertFalse(location2.exists());
    try (Reader reader = new InputStreamReader(location.getInputStream(), Charsets.UTF_8)) {
      Assert.assertEquals(message, CharStreams.toString(reader));
    }
    Assert.assertEquals(length, location.length());
    Assert.assertEquals(lastModified, location.lastModified());
  }

  @Test
  public void testDelete() throws IOException {
    LocationFactory factory = locationFactoryCache.getUnchecked("delete");

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
    LocationFactory factory = LocationFactories.namespace(locationFactoryCache.getUnchecked("helper"), "testhelper");

    Location location = factory.create("test");
    Assert.assertTrue(location.toURI().getPath().endsWith("testhelper/test"));

    location = factory.create(URI.create("test2"));
    Assert.assertTrue(location.toURI().getPath().endsWith("testhelper/test2"));
  }

  @Test
  public void testList() throws IOException {
    LocationFactory factory = locationFactoryCache.getUnchecked("list");

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

  protected abstract LocationFactory createLocationFactory(String pathBase) throws Exception;
}
