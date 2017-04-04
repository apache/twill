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

import com.google.common.base.Throwables;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Threads;
import org.junit.Assert;
import org.junit.Test;

import java.io.PrintWriter;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Testing application master will shutdown itself when all tasks are completed.
 * This test is executed by {@link YarnTestSuite}.
 */
public final class TaskCompletedTestRun extends BaseYarnTest {

  /**
   * A {@link AbstractTwillRunnable} that sleeps randomly and finish.
   */
  public static final class SleepTask extends AbstractTwillRunnable {

    @Override
    public void run() {
      // Randomly sleep for 3-5 seconds.
      try {
        TimeUnit.SECONDS.sleep(new Random().nextInt(3) + 3);
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void stop() {
      // No-op
    }
  }

  @Test
  public void testTaskCompleted() throws InterruptedException, TimeoutException, ExecutionException {
    TwillRunner twillRunner = getTwillRunner();
    TwillController controller = twillRunner.prepare(new SleepTask(),
                                                ResourceSpecification.Builder.with()
                                                  .setVirtualCores(1)
                                                  .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
                                                  .setInstances(3).build())
                                            .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                                            .start();

    final CountDownLatch runLatch = new CountDownLatch(1);
    controller.onRunning(new Runnable() {
      @Override
      public void run() {
        runLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(runLatch.await(1, TimeUnit.MINUTES));
    controller.awaitTerminated(1, TimeUnit.MINUTES);
    Assert.assertEquals(ServiceController.TerminationStatus.SUCCEEDED, controller.getTerminationStatus());
  }

  @Test
  public void testFailureComplete() throws TimeoutException, ExecutionException, InterruptedException {
    TwillRunner twillRunner = getTwillRunner();

    // Start the app with an invalid ClassLoader. This will cause the AM fails to start.
    TwillController controller = twillRunner.prepare(new SleepTask(),
                                                     ResourceSpecification.Builder.with()
                                                       .setVirtualCores(1)
                                                       .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
                                                       .setInstances(1).build())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .setClassLoader("InvalidClassLoader")
      .start();

    final CountDownLatch terminateLatch = new CountDownLatch(1);
    controller.onTerminated(new Runnable() {
      @Override
      public void run() {
        terminateLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(terminateLatch.await(2, TimeUnit.MINUTES));
    Assert.assertEquals(ServiceController.TerminationStatus.FAILED, controller.getTerminationStatus());
  }
}
