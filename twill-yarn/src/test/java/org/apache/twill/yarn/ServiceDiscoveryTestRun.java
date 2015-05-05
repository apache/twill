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
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for ability to discover existence of services through {@link TwillContext}.
 */
public final class ServiceDiscoveryTestRun extends BaseYarnTest {

  @Test
  public void testServiceDiscovery() throws InterruptedException, ExecutionException, TimeoutException {
    TwillRunner twillRunner = YarnTestUtils.getTwillRunner();
    TwillController controller = twillRunner
      .prepare(new ServiceApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .withArguments("r1", "12345")
      .withArguments("r2", "45678")
      .start();


    controller.awaitTerminated(120, TimeUnit.SECONDS);
  }

  /**
   * An application that contains two {@link ServiceRunnable}.
   */
  public static final class ServiceApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("ServiceApp")
        .withRunnable()
          .add("r1", new ServiceRunnable()).noLocalFiles()
          .add("r2", new ServiceRunnable()).noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  /**
   * A Runnable that will announce on service and wait for announcement from another instance in the same service.
   */
  public static final class ServiceRunnable extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(ServiceRunnable.class);
    private static final String SERVICE_NAME = "service";
    private volatile Thread runThread;

    @Override
    public void run() {
      this.runThread = Thread.currentThread();
      final int port = Integer.parseInt(getContext().getArguments()[0]);
      getContext().announce(SERVICE_NAME, port);

      final CountDownLatch discoveredLatch = new CountDownLatch(1);

      ServiceDiscovered serviceDiscovered = getContext().discover(SERVICE_NAME);
      serviceDiscovered.watchChanges(new ServiceDiscovered.ChangeListener() {
        @Override
        public void onChange(ServiceDiscovered serviceDiscovered) {
          // Try to find a discoverable that is not this instance
          for (Discoverable discoverable : serviceDiscovered) {
            int discoveredPort = discoverable.getSocketAddress().getPort();
            if (SERVICE_NAME.equals(discoverable.getName()) && discoveredPort != port) {
              LOG.info("{}: Service discovered at {}", getContext().getSpecification().getName(), discoveredPort);
              discoveredLatch.countDown();
            }
          }
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      try {
        discoveredLatch.await();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted.", e);
      }
    }

    @Override
    public void stop() {
      if (runThread != null) {
        runThread.interrupt();
      }
    }
  }
}
