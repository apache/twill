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

import com.google.common.base.Stopwatch;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.PrinterLogHandler;

import org.apache.twill.common.Threads;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test class whether enable certain log level for application container works.
 */
public class LogLevelTestRun extends BaseYarnTest {

  /**
   * A test of TwillRunnable to see if the DEBUG log level is enabled.
   * */
  public static final class LogLevelTestRunnable extends AbstractTwillRunnable {
    public static final Logger LOG = LoggerFactory.getLogger(LogLevelTestRunnable.class);

    private volatile Thread runThread;

    @Override
    public void run() {
      this.runThread = Thread.currentThread();
      while (!Thread.interrupted()) {
        // check if DEBUG log level is enabled
        boolean isDebug = LOG.isDebugEnabled();

        Assert.assertTrue(isDebug);

        try {
          TimeUnit.MILLISECONDS.sleep(200);
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

  /**
   * A test TwillApplication to test setting log level to DEBUG.
   */
  public static final class LogLevelTestApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName(LogLevelTestApplication.class.getSimpleName())
        .withRunnable()
        .add(LogLevelTestRunnable.class.getSimpleName(), new LogLevelTestRunnable()).noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  @Test
  public void testDebugLogLevel()throws Exception {
    YarnTwillRunnerService runner = getTwillRunner();
    runner.start();

    // Set log level to DEBUG
    TwillController controller = runner.prepare(new LogLevelTestApplication())
      .setLogLevel(LogEntry.Level.DEBUG)
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
      .start();

    // Lets wait until the service is running
    final CountDownLatch running = new CountDownLatch(1);
    controller.onRunning(new Runnable() {
      @Override
      public void run() {
        running.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    Assert.assertTrue(running.await(200, TimeUnit.SECONDS));

    LogEntry.Level logLevel = waitForLogLevel(controller, LogLevelTestRunnable.class.getSimpleName(), 30L,
                                              TimeUnit.SECONDS);

    // Verify we got DEBUG log level.
    Assert.assertEquals(LogEntry.Level.DEBUG, logLevel);

    controller.terminate().get(120, TimeUnit.SECONDS);

    // Sleep a bit for full cleanup
    TimeUnit.SECONDS.sleep(2);
  }

  // Need helper method here to wait for getting resource report because {@link TwillController#getResourceReport()}
  // could return null if the application has not fully started.
  private LogEntry.Level waitForLogLevel(TwillController controller, String runnable, long timeout,
                                         TimeUnit timeoutUnit) throws InterruptedException {

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    do {
      ResourceReport report = controller.getResourceReport();
      if (report == null || report.getRunnableResources(runnable) == null) {
        continue;
      }
      for (TwillRunResources resources : report.getRunnableResources(runnable)) {
        if (resources.getLogLevel() != null) {
           return resources.getLogLevel();
        }
      }
      TimeUnit.MILLISECONDS.sleep(100);
    } while (stopwatch.elapsedTime(timeoutUnit) < timeout);

    return null;
  }
}
