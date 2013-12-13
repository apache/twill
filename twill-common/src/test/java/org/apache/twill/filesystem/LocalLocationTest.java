/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.filesystem;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;

/**
 *
 */
public class LocalLocationTest {

  @Test
  public void testDelete() throws IOException {
    LocationFactory factory = new LocalLocationFactory(new File(System.getProperty("java.io.tmpdir")));

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
    LocationFactory factory = LocationFactories.namespace(
                                new LocalLocationFactory(new File(System.getProperty("java.io.tmpdir"))),
                                "testhelper");

    Location location = factory.create("test");
    Assert.assertTrue(location.toURI().getPath().endsWith("testhelper/test"));

    location = factory.create(URI.create("test2"));
    Assert.assertTrue(location.toURI().getPath().endsWith("testhelper/test2"));
  }
}
