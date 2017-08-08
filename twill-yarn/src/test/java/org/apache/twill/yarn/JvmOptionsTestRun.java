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

import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/**
 * Unit test for testing extra JVM options setting for runnables.
 */
public class JvmOptionsTestRun extends BaseYarnTest {

  @Test
  public void testExtraOptions() throws InterruptedException, ExecutionException {
    // Start the testing app with jvm options at both global level as well as for the specific runnables.
    TwillController controller = getTwillRunner()
      .prepare(new JvmOptionsApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
      .setJVMOptions("-Dservice.name=default")
      .setJVMOptions("r2", "-Dservice.name=r2")
      .start();

    // For r1 and r3 will be using "default" as the service name.
    waitForSize(controller.discoverService("default"), 2, 120);
    // r2 will be use "r2" as the service name.
    waitForSize(controller.discoverService("r2"), 1, 120);

    controller.terminate().get();
  }

  /**
   * Application for testing extra jvm options
   */
  public static final class JvmOptionsApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName(JvmOptionsApplication.class.getSimpleName())
        .withRunnable()
          .add("r1", new SimpleRunnable()).noLocalFiles()
          .add("r2", new SimpleRunnable()).noLocalFiles()
          .add("r3", new SimpleRunnable()).noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  /**
   * A runnable that simple announce itself to some name based on the system property and wait for stop signal.
   */
  public static final class SimpleRunnable extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleRunnable.class);

    private final CountDownLatch stopLatch = new CountDownLatch(1);

    @Override
    public void run() {
      String runnableName = getContext().getSpecification().getName();
      String serviceName = System.getProperty("service.name");
      LOG.info("Announcing with name {} for runnable {}", serviceName, runnableName);

      // Compute a unique port name based on runnable name (running names are r[0-9]+)
      getContext().announce(serviceName, 12345 + Integer.parseInt(runnableName.substring(1)));
      try {
        stopLatch.await();
      } catch (InterruptedException e) {
        LOG.warn("Run thread interrupted", e);
      }
    }

    @Override
    public void stop() {
      stopLatch.countDown();
    }
  }
}
