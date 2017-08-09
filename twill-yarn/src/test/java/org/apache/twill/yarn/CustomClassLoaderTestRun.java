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
package org.apache.twill.yarn;

import org.apache.twill.api.TwillController;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.junit.Assert;
import org.junit.Test;

import java.io.PrintWriter;

/**
 * Unit test for testing custom classloader for containers.
 */
public class CustomClassLoaderTestRun extends BaseYarnTest {

  @Test
  public void testCustomClassLoader() throws Exception {
    TwillController controller = getTwillRunner().prepare(new CustomClassLoaderRunnable())
      .setClassLoader(CustomClassLoader.class.getName())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .setJVMOptions("-Dservice.port=54321")
      .setJVMOptions(CustomClassLoaderRunnable.class.getSimpleName(), "-Dservice.name=custom")
      .start();

    Assert.assertTrue(waitForSize(controller.discoverService("custom"), 1, 120));
    controller.terminate().get();
  }
}
