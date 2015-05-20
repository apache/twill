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
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Threads;
import org.junit.Assert;
import org.junit.Test;

import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests whether starting containers with debugging on works.
 */
public class DebugTestRun extends BaseYarnTest {

  /**
   * An application that contains two {@link DummyRunnable}.
   */
  public static final class DummyApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
                                       .setName("DummyApp")
                                       .withRunnable()
                                       .add("r1", new DummyRunnable()).noLocalFiles()
                                       .add("r2", new DummyRunnable()).noLocalFiles()
                                       .anyOrder()
                                       .build();
    }
  }

  /**
   * A Runnable that will sleep in a loop until stopped.
   */
  public static final class DummyRunnable extends AbstractTwillRunnable {

    private volatile Thread runThread;

    @Override
    public void run() {
      this.runThread = Thread.currentThread();
      while (!Thread.interrupted()) {
        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException e) {
          break;
        }
      }
    }

    @Override
    public void stop() {
      if (runThread != null) {
        runThread.interrupt();
      }
    }
  }

  private boolean waitForDebugPort(TwillController controller, String runnable, int timeLimit)
    throws InterruptedException {
    long millis = 0;
    while (millis < 1000 * timeLimit) {
      ResourceReport report = controller.getResourceReport();
      if (report == null || report.getRunnableResources(runnable) == null) {
        continue;
      }
      for (TwillRunResources resources : report.getRunnableResources(runnable)) {
        if (resources.getDebugPort() != null) {
          return true;
        }
      }
      TimeUnit.MILLISECONDS.sleep(100);
      millis += 100;
    }
    return false;
  }

  @Test
  public void testDebugPortOneRunnable() throws Exception {
    YarnTwillRunnerService runner = getTwillRunner();
    runner.start();

    TwillController controller = runner.prepare(new DummyApplication())
                                       .enableDebugging("r1")
                                       .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
                                       .start();
    final CountDownLatch running = new CountDownLatch(1);
    controller.onRunning(new Runnable() {
      @Override
      public void run() {
        running.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(running.await(120, TimeUnit.SECONDS));
    Assert.assertTrue(waitForDebugPort(controller, "r1", 30));
    controller.terminate().get(120, TimeUnit.SECONDS);
    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }

  @Test
  public void testDebugPortAllRunnables() throws Exception {
    YarnTwillRunnerService runner = getTwillRunner();
    runner.start();

    TwillController controller = runner.prepare(new DummyApplication())
                                       .enableDebugging()
                                       .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
                                       .start();
    final CountDownLatch running = new CountDownLatch(1);
    controller.onRunning(new Runnable() {
      @Override
      public void run() {
        running.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(running.await(120, TimeUnit.SECONDS));
    Assert.assertTrue(waitForDebugPort(controller, "r1", 30));
    Assert.assertTrue(waitForDebugPort(controller, "r2", 30));
    controller.terminate().get(120, TimeUnit.SECONDS);
    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }
}
