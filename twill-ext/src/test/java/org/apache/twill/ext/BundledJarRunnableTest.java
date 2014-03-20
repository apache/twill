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
package org.apache.twill.ext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@link BundledJarRunnable}.
 */
public class BundledJarRunnableTest {

  @Test
  public void testArgumentsOperations() {
    String[] mainArgs = {"a", "b", "c"};
    String jarFileName = "test/123.jar";
    String libFolder = "libsdf";
    String mainClassName = "org.apache.twill.ext.Test";

    List<String> argsArrayList = ImmutableList.<String>builder()
      .add(jarFileName)
      .add(libFolder)
      .add(mainClassName)
      .addAll(Lists.newArrayList(mainArgs))
      .build();

    String[] argsArray = argsArrayList.toArray(new String[argsArrayList.size()]);

    BundledJarRunner.Arguments args = new BundledJarRunner.Arguments.Builder()
      .setMainArgs(mainArgs)
      .setJarFileName(jarFileName)
      .setLibFolder(libFolder)
      .setMainClassName(mainClassName)
      .createArguments();

    Assert.assertArrayEquals(mainArgs, args.getMainArgs());
    Assert.assertEquals(jarFileName, args.getJarFileName());
    Assert.assertEquals(libFolder, args.getLibFolder());
    Assert.assertEquals(mainClassName, args.getMainClassName());

    String[] array = args.toArray();
    Assert.assertArrayEquals(argsArray, array);
    Assert.assertEquals(args, BundledJarRunner.Arguments.fromArray(array));
  }
}
