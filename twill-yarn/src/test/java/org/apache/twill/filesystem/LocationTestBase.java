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
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
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
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
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
  public void testHomeLocation() throws Exception {
    final LocationFactory locationFactory = createLocationFactory("/");

    // Without UGI, the home location should be the same as the current user
    UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();
    Assert.assertEquals(currentUser.getShortUserName(), locationFactory.getHomeLocation().getName());

    // With UGI, the home location should be based on the UGI current user
    UserGroupInformation ugi = createTestUGI();

    Location homeLocation = ugi.doAs(new PrivilegedAction<Location>() {
      @Override
      public Location run() {
        return locationFactory.getHomeLocation();
      }
    });
    Assert.assertEquals(ugi.getUserName(), homeLocation.getName());
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

  @Test
  public void testOwnerGroup() throws Exception {
    final LocationFactory factory = locationFactoryCache.getUnchecked("ownergroup");

    UserGroupInformation testUGI = createTestUGI();
    Location location = testUGI.doAs(new PrivilegedExceptionAction<Location>() {
      @Override
      public Location run() throws Exception {
        return factory.create("ogtest");
      }
    });

    location.createNew();
    Assert.assertEquals(testUGI.getUserName(), location.getOwner());

    String group = testUGI.getGroupNames()[0];

    location.setGroup(group);
    Assert.assertEquals(group, location.getGroup());
  }

  @Test
  public void testPermissions() throws IOException, InterruptedException {
    createTestUGI().doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        LocationFactory factory = locationFactoryCache.getUnchecked("permission1");

        // Test permissions setting on createNew calls
        Location location = factory.create("test400-1");
        Assert.assertTrue(location.createNew("400"));
        Assert.assertEquals("r--------", location.getPermissions());
        location = factory.create("test400-2");
        Assert.assertTrue(location.createNew("r--------"));
        Assert.assertEquals("r--------", location.getPermissions());
        Assert.assertFalse(location.createNew("600"));

        location = factory.create("test660-1");
        Assert.assertTrue(location.createNew("660"));
        Assert.assertEquals("rw-rw----", location.getPermissions());
        location = factory.create("test660-2");
        Assert.assertTrue(location.createNew("rw-rw----"));
        Assert.assertEquals("rw-rw----", location.getPermissions());
        Assert.assertFalse(location.createNew("600"));

        location = factory.create("test644-1");
        Assert.assertTrue(location.createNew("644"));
        Assert.assertEquals("rw-r--r--", location.getPermissions());
        location = factory.create("test644-2");
        Assert.assertTrue(location.createNew("rw-r--r--"));
        Assert.assertEquals("rw-r--r--", location.getPermissions());
        Assert.assertFalse(location.createNew("600"));

        // Test permissions setting on getOutputStream calls
        factory = locationFactoryCache.getUnchecked("permission2");

        location = factory.create("test400-1");
        location.getOutputStream("400").close();
        Assert.assertEquals("r--------", location.getPermissions());
        location = factory.create("test400-2");
        location.getOutputStream("r--------").close();
        Assert.assertEquals("r--------", location.getPermissions());

        location = factory.create("test660-1");
        location.getOutputStream("660").close();
        Assert.assertEquals("rw-rw----", location.getPermissions());
        location = factory.create("test660-2");
        location.getOutputStream("rw-rw----").close();
        Assert.assertEquals("rw-rw----", location.getPermissions());

        location = factory.create("test644-1");
        location.getOutputStream("644").close();
        Assert.assertEquals("rw-r--r--", location.getPermissions());
        location = factory.create("test644-2");
        location.getOutputStream("rw-r--r--").close();
        Assert.assertEquals("rw-r--r--", location.getPermissions());

        // Test permissions setting on setPermission method
        factory = locationFactoryCache.getUnchecked("permission3");

        // Setting permission on non-existed file should have IOException thrown
        location = factory.create("somefile");
        try {
          location.setPermissions("400");
          Assert.fail("IOException expected on setting permission on non-existing Location.");
        } catch (IOException e) {
          // expected
        }

        // Create file with read only permission
        Assert.assertTrue(location.createNew("444"));
        Assert.assertEquals("r--r--r--", location.getPermissions());
        // Change the permission to write only
        location.setPermissions("222");
        Assert.assertEquals("-w--w--w-", location.getPermissions());

        return null;
      }
    });
  }

  @Test
  public void testDirPermissions() throws IOException, InterruptedException {
    createTestUGI().doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {

        LocationFactory factory = locationFactoryCache.getUnchecked("permissionD");

        Location location = factory.create("base");
        String permissions = "rwxr-x---";
        location.mkdirs(permissions);
        Assert.assertTrue(location.exists());
        Assert.assertTrue(location.isDirectory());
        Assert.assertEquals(permissions, location.getPermissions());

        permissions = "rwx------";
        location.setPermissions(permissions);
        Assert.assertEquals(permissions, location.getPermissions());

        Location child = location.append("child");
        Location grandchild = child.append("grandchild");
        permissions = "rwx-w--w-";
        grandchild.mkdirs(permissions);
        Assert.assertTrue(child.isDirectory());
        Assert.assertTrue(grandchild.isDirectory());
        Assert.assertEquals(permissions, child.getPermissions());
        Assert.assertEquals(permissions, grandchild.getPermissions());

        permissions = "rwx------";
        child.delete(true);
        Assert.assertFalse(child.exists());
        Location textfile = grandchild.append("a.txt");
        textfile.getOutputStream(permissions).close();
        Assert.assertTrue(child.isDirectory());
        Assert.assertTrue(grandchild.isDirectory());
        Assert.assertFalse(textfile.isDirectory());
        Assert.assertEquals(permissions, child.getPermissions());
        Assert.assertEquals(permissions, grandchild.getPermissions());
        Assert.assertEquals(correctFilePermissions(permissions), textfile.getPermissions());

        // mkdirs of existing file
        Location file = factory.create("existingfile");
        Assert.assertTrue(file.createNew("rwx------"));
        Assert.assertFalse(file.mkdirs());
        Assert.assertFalse(file.mkdirs("rwxrwx---"));

        // mkdirs where parent is existing file
        file = file.append("newdir");
        Assert.assertFalse(file.mkdirs());
        Assert.assertFalse(file.mkdirs("rwxrwx---"));

        // mkdirs of existing directory
        Location dir = factory.create("existingdir");
        Assert.assertTrue(dir.mkdirs());
        Assert.assertFalse(dir.mkdirs());
        Assert.assertFalse(dir.mkdirs("rwxrwx---"));

        // mkdirs for existing parent with different permissions -> should not change
        dir.setPermissions("rwx------");
        Assert.assertEquals("rwx------", dir.getPermissions());
        Location newdir = dir.append("newdir");
        Assert.assertTrue(newdir.mkdirs("rwxrwx---"));
        Assert.assertEquals("rwxrwx---", newdir.getPermissions());
        Assert.assertEquals("rwx------", dir.getPermissions());

        // mkdirs whithout permission for parent
        Assert.assertTrue(newdir.delete(true));
        dir.setPermissions("r-x------");
        Assert.assertEquals("r-x------", dir.getPermissions());
        try {
          Assert.assertFalse(newdir.mkdirs());
        } catch (AccessControlException e) {
          // expected
        }
        try {
          Assert.assertFalse(newdir.mkdirs("rwxrwx---"));
        } catch (AccessControlException e) {
          // expected
        }
        return null;
      }
    });
  }

  /**
   * Create a location factory rooted at a given path.
   */
  protected abstract LocationFactory createLocationFactory(String pathBase) throws Exception;

  /**
   * Some older versions of Hadoop always strip the execute permission from files (but keep it for directories).
   * This allows subclasses to correct the expected file permissions, based on the Hadoop version (if any).
   */
  protected String correctFilePermissions(String expectedFilePermissions) {
    return expectedFilePermissions; // unchanged by default
  }

  protected UserGroupInformation createTestUGI() throws IOException {
    String userName = System.getProperty("user.name").equals("tester") ? "twiller" : "tester";
    return UserGroupInformation.createUserForTesting(userName, new String[] { "testgroup" });
  }
}
