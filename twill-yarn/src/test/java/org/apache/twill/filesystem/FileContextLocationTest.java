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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.internal.yarn.YarnUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 *
 */
public class FileContextLocationTest extends LocationTestBase {

  public static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void init() throws IOException {
    Configuration conf = new Configuration();
    conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, tmpFolder.newFolder().getAbsolutePath());
    dfsCluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    // make root world-writable so that we can create all location factories as unprivileged user
    dfsCluster.getFileSystem().setPermission(new Path("/"), FsPermission.valueOf("-rwxrwxrwx"));
  }

  @AfterClass
  public static void finish() {
    dfsCluster.shutdown();
  }

  @Override
  protected LocationFactory createLocationFactory(String pathBase) throws Exception {
    return new FileContextLocationFactory(dfsCluster.getFileSystem().getConf(), pathBase);
  }

  @Override
  protected String correctFilePermissions(String original) {
    if (YarnUtils.HadoopVersions.HADOOP_20.equals(YarnUtils.getHadoopVersion())) {
      return original.substring(0, 2) + '-' + // strip the x for owner
        original.substring(3, 5) + '-' + // strip the x for group
        original.substring(6, 8) + '-'; // strip the x for world;
    }
    return original;
  }

  @Test
  public void testGetFileContext() throws Exception {
    final FileContextLocationFactory locationFactory = (FileContextLocationFactory) createLocationFactory("/testGetFC");

    Assert.assertEquals(UserGroupInformation.getCurrentUser(), locationFactory.getFileContext().getUgi());

    UserGroupInformation testUGI = createTestUGI();
    FileContext fileContext = testUGI.doAs(new PrivilegedExceptionAction<FileContext>() {
      @Override
      public FileContext run() throws Exception {
        return locationFactory.getFileContext();
      }
    });
    Assert.assertEquals(testUGI, fileContext.getUgi());
  }
}
