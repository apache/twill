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

import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 *
 */
public class FileContextLocationTest extends LocationTestBase {

  public static MiniDFSCluster dfsCluster;
  private static UserGroupInformation testUGI;

  @BeforeClass
  public static void init() throws IOException {
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.newFolder().getAbsolutePath());
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    // make root world-writable so that we can create all location factories as unprivileged user
    dfsCluster.getFileSystem().setPermission(new Path("/"), FsPermission.valueOf("-rwxrwxrwx"));
    // to run these tests not as superuser, make sure to use a user name other than the JVM user
    String userName = System.getProperty("user.name").equals("tester") ? "twiller" : "tester";
    testUGI = UserGroupInformation.createUserForTesting(userName, new String[] { "testgroup" });
  }

  @AfterClass
  public static void finish() {
    dfsCluster.shutdown();
  }

  @Override
  protected String getUserName() {
    return testUGI.getUserName();
  }

  @Override
  protected String getUserGroup(String ignoredGroupName) {
    return testUGI.getGroupNames()[testUGI.getGroupNames().length - 1];
  }

  @Override
  protected LocationFactory createLocationFactory(String pathBase) throws Exception {
    return createLocationFactory(pathBase, testUGI);
  }

  @Override
  protected LocationFactory createLocationFactory(final String pathBase, UserGroupInformation ugi) throws Exception {
    LocationFactory factory = ugi.doAs(new PrivilegedAction<LocationFactory>() {
      @Override
      public LocationFactory run() {
        try {
          return doCreateLocationFactory(pathBase);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    });
    // make sure the root of the location factory exists and only permits the test user
    Location root = factory.create("/");
    root.mkdirs("rwx------");
    return factory;
  }

  protected LocationFactory doCreateLocationFactory(String pathBase) throws IOException {
    return new FileContextLocationFactory(dfsCluster.getFileSystem().getConf(), pathBase);
  }
}
