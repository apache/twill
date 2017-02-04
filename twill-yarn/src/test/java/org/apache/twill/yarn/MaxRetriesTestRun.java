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
package org.apache.twill.yarn;

import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for RunningContainers class.
 */
public final class MaxRetriesTestRun extends BaseYarnTest {
   
  @Test
  public void maxRetriesOneInstance() throws TimeoutException, ExecutionException {
    maxRetriesRun(1);
  }

  @Test
  public void maxRetriesTwoInstances() throws TimeoutException, ExecutionException {
    maxRetriesRun(2);
  }

  private void maxRetriesRun(final int instances) throws TimeoutException, ExecutionException {
    TwillRunner runner = getTwillRunner();
    final int maxRetries = 3;
    final AtomicInteger retriesSeen = new AtomicInteger(0);

    ResourceSpecification resource = ResourceSpecification.Builder.with().setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA).setInstances(instances).build();

    TwillController controller = runner.prepare(new FailingServer(), resource)
      .withMaxRetries(FailingServer.class.getSimpleName(), maxRetries)
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .addLogHandler(new LogHandler() {
        @Override
        public void onLog(LogEntry logEntry) {
          if (logEntry.getMessage().contains("retries for instance")) {
            retriesSeen.incrementAndGet();
          }
        }
      })
      .start();

    controller.awaitTerminated(2, TimeUnit.MINUTES);
    Assert.assertEquals(maxRetries * instances, retriesSeen.get());
  }

  @Test
  public void maxRetriesWithIncreasedInstances() throws InterruptedException, ExecutionException {
    TwillRunner runner = getTwillRunner();
    final int maxRetries = 3;
    final AtomicInteger retriesSeen = new AtomicInteger(0);
    final CountDownLatch retriesExhausted = new CountDownLatch(1);
    final CountDownLatch allRunning = new CountDownLatch(1);

    // start with 2 instances
    ResourceSpecification resource = ResourceSpecification.Builder.with().setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA).setInstances(2).build();

    TwillController controller = runner.prepare(new FailingInstanceServer(), resource)
      .withMaxRetries(FailingInstanceServer.class.getSimpleName(), maxRetries)
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true))).addLogHandler(new LogHandler() {
        @Override
        public void onLog(LogEntry logEntry) {
          if (logEntry.getMessage().contains(" retries for instance")) {
            retriesSeen.incrementAndGet();
          }
          if (logEntry.getMessage().contains("Retries exhausted")) {
            retriesExhausted.countDown();
          }
          if (logEntry.getMessage().contains("fully provisioned with 2 instances")) {
            allRunning.countDown();
          }
        }
      }).start();

    try {
      // wait for initial instances to have started
      allRunning.await();

      /*
       * now increase the number of instances. these should fail since there instance ids are > 1. afterwards, the
       * number of retries should be 3 since only this one instance failed.
       */
      controller.changeInstances(FailingInstanceServer.class.getSimpleName(), 3);
      retriesExhausted.await();
      Assert.assertEquals(3, retriesSeen.get());

    } finally {
      controller.terminate().get();
    }
  }

  /**
   * A runnable that throws an exception during run().
   */
  public static final class FailingServer extends AbstractTwillRunnable {
    private static final Logger LOG = LoggerFactory.getLogger(FailingServer.class);

    @Override
    public void run() {
      LOG.info("Failing instance {}", getContext().getInstanceId());
      throw new RuntimeException("FAIL early FAIL often");
    }
  }

  /**
   * A runnable that throws an exception during run() but only for instance ids > 1.
   */
  public static final class FailingInstanceServer extends AbstractTwillRunnable {
    private static final Logger LOG = LoggerFactory.getLogger(FailingServer.class);

    @Override
    public void run() {
      if (getContext().getInstanceId() > 1) {
        LOG.info("Failing instance {}", getContext().getInstanceId());
        throw new RuntimeException("FAIL early FAIL often");
      } else {
        LOG.info("Instance {} is running", getContext().getInstanceId());
        while (true) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            return;
          }
        }
      }
    }
  }

}
