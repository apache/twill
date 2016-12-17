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
import com.google.common.collect.ImmutableMap;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test changing log level for a twill runnable.
 */
public class LogLevelChangeTestRun extends BaseYarnTest {
  public static final Logger LOG = LoggerFactory.getLogger(LogLevelChangeTestRun.class);

  /**
   * Twill runnable.
   */
  public static final class LogLevelTestRunnable extends AbstractTwillRunnable {
    public static final Logger LOG = LoggerFactory.getLogger(LogLevelTestRunnable.class);

    private volatile Thread runThread;

    @Override
    public void run() {
      this.runThread = Thread.currentThread();

      // check if the initial log level is DEBUG
      Assert.assertTrue(LOG.isDebugEnabled() && !LOG.isTraceEnabled());

      int i = 0;
      while (!Thread.interrupted()) {
        if (i == 0 && !LOG.isDebugEnabled()) {
          // check if the log level is changed to INFO
          Assert.assertTrue(LOG.isInfoEnabled());
          i++;
        }
        if (i == 1 && !LOG.isInfoEnabled()) {
          // check if the log level is changed to WARN
          Assert.assertTrue(LOG.isWarnEnabled());
          i++;
        }

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

  /**
   * Second runnable.
   */
  public static final class LogLevelTestSecondRunnable extends AbstractTwillRunnable {
    public static final Logger LOG = LoggerFactory.getLogger(LogLevelTestSecondRunnable.class);

    private volatile Thread runThread;

    @Override
    public void run() {
      this.runThread = Thread.currentThread();

      int i = 0;
      while (!Thread.interrupted()) {
        if (i == 0 && !LOG.isDebugEnabled()) {
          // check if the log level is changed to INFO
          Assert.assertTrue(LOG.isInfoEnabled());
          i++;
        }
        if (i == 1 && LOG.isDebugEnabled()) {
          // check if the log level is changed to TRACE
          Assert.assertTrue(LOG.isTraceEnabled());
          i++;
        }

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

  /**
   * A test TwillApplication to test setting log level to DEBUG.
   */
  public static final class LogLevelTestApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("LogLevelChangeTest")
        .withRunnable()
        .add(LogLevelTestRunnable.class.getSimpleName(), new LogLevelTestRunnable()).noLocalFiles()
        .add(LogLevelTestSecondRunnable.class.getSimpleName(), new LogLevelTestSecondRunnable()).noLocalFiles()
        .anyOrder()
        .build();
    }

  }

  @Test
  public void testChangeLogLevel() throws Exception {
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

    // assert that log level is DEBUG
    waitForLogLevel(controller, LogLevelTestRunnable.class.getSimpleName(),
                    20L, TimeUnit.SECONDS, LogEntry.Level.DEBUG, ImmutableMap.of("ROOT", LogEntry.Level.DEBUG));

    waitForLogLevel(controller, LogLevelTestSecondRunnable.class.getSimpleName(),
                    20L, TimeUnit.SECONDS, LogEntry.Level.DEBUG, ImmutableMap.of("ROOT", LogEntry.Level.DEBUG));

    // change the log level to INFO
    controller.updateLogLevels(ImmutableMap.of(Logger.ROOT_LOGGER_NAME, LogEntry.Level.INFO)).get();

    // assert log level has changed to INFO
    waitForLogLevel(controller, LogLevelTestRunnable.class.getSimpleName(),
                    20L, TimeUnit.SECONDS, LogEntry.Level.INFO, ImmutableMap.of("ROOT", LogEntry.Level.INFO));

    waitForLogLevel(controller, LogLevelTestSecondRunnable.class.getSimpleName(),
                    20L, TimeUnit.SECONDS, LogEntry.Level.INFO, ImmutableMap.of("ROOT", LogEntry.Level.INFO));

    // change the log level of LogLevelTestRunnable to WARN,
    // change the log level of LogLevelTestSecondRunnable to TRACE
    Map<String, LogEntry.Level> logLevelFirstRunnable = ImmutableMap.of(Logger.ROOT_LOGGER_NAME, LogEntry.Level.WARN);
    Map<String, LogEntry.Level> logLevelSecondRunnable = ImmutableMap.of(Logger.ROOT_LOGGER_NAME, LogEntry.Level.TRACE);
    controller.updateLogLevels(LogLevelTestRunnable.class.getSimpleName(), logLevelFirstRunnable).get();
    controller.updateLogLevels(LogLevelTestSecondRunnable.class.getSimpleName(), logLevelSecondRunnable).get();

    waitForLogLevel(controller, LogLevelTestRunnable.class.getSimpleName(),
                    20L, TimeUnit.SECONDS, LogEntry.Level.WARN, ImmutableMap.of("ROOT", LogEntry.Level.WARN));
    waitForLogLevel(controller, LogLevelTestSecondRunnable.class.getSimpleName(),
                    20L, TimeUnit.SECONDS, LogEntry.Level.TRACE, ImmutableMap.of("ROOT", LogEntry.Level.TRACE));

    // change a particular logger to log level warn and reset it back.
    logLevelFirstRunnable = ImmutableMap.of("test", LogEntry.Level.WARN);
    controller.updateLogLevels(LogLevelTestRunnable.class.getSimpleName(), logLevelFirstRunnable).get();
    waitForLogLevel(controller, LogLevelTestRunnable.class.getSimpleName(),
                    20L, TimeUnit.SECONDS, LogEntry.Level.WARN,
                    ImmutableMap.of("ROOT", LogEntry.Level.WARN, "test", LogEntry.Level.WARN));
    logLevelFirstRunnable = new HashMap<>();
    logLevelFirstRunnable.put("test", null);
    controller.updateLogLevels(LogLevelTestRunnable.class.getSimpleName(), logLevelFirstRunnable).get();
    Map<String, LogEntry.Level> result = new HashMap<>();
    result.put("ROOT", LogEntry.Level.WARN);
    result.put("test", null);
    waitForLogLevel(controller, LogLevelTestRunnable.class.getSimpleName(),
                    20L, TimeUnit.SECONDS, LogEntry.Level.WARN, result);

    // reset the log level for a particular logger of LogLevelTestRunnable
    controller.resetRunnableLogLevels(LogLevelTestRunnable.class.getSimpleName(), "test").get();
    result.remove("test");
    waitForLogLevel(controller, LogLevelTestRunnable.class.getSimpleName(),
                    20L, TimeUnit.SECONDS, LogEntry.Level.WARN, result);

    // change the log level of LogLevelTestSecondRunnable to INFO and change instances of it to test if the log level
    // request get applied to container started up later
    logLevelSecondRunnable = ImmutableMap.of(Logger.ROOT_LOGGER_NAME, LogEntry.Level.INFO, "test",
                                             LogEntry.Level.WARN);
    controller.updateLogLevels(LogLevelTestSecondRunnable.class.getSimpleName(), logLevelSecondRunnable).get();
    controller.changeInstances(LogLevelTestSecondRunnable.class.getSimpleName(), 2).get();
    TimeUnit.SECONDS.sleep(5);
    waitForLogLevel(controller, LogLevelTestSecondRunnable.class.getSimpleName(), 20L, TimeUnit.SECONDS,
                    LogEntry.Level.INFO, logLevelSecondRunnable);

    // reset the log levels back to default.
    controller.resetLogLevels().get();
    waitForLogLevel(controller, LogLevelTestRunnable.class.getSimpleName(),
                    20L, TimeUnit.SECONDS, LogEntry.Level.DEBUG, ImmutableMap.of("ROOT", LogEntry.Level.DEBUG));
    waitForLogLevel(controller, LogLevelTestSecondRunnable.class.getSimpleName(),
                    20L, TimeUnit.SECONDS, LogEntry.Level.DEBUG, ImmutableMap.of("ROOT", LogEntry.Level.DEBUG));

    // stop
    controller.terminate().get(120, TimeUnit.SECONDS);

    // Sleep a bit for full cleanup
    TimeUnit.SECONDS.sleep(2);
  }

  // Need helper method here to wait for getting resource report because {@link TwillController#getResourceReport()}
  // could return null if the application has not fully started.
  private void waitForLogLevel(TwillController controller, String runnable, long timeout,
                               TimeUnit timeoutUnit, LogEntry.Level expected,
                               Map<String, LogEntry.Level> expectedArgs) throws InterruptedException {

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    LogEntry.Level actual = null;
    Map<String, LogEntry.Level> actualArgs = null;
    boolean stopped = false;
    do {
      ResourceReport report = controller.getResourceReport();
      if (report == null || report.getRunnableResources(runnable) == null) {
        continue;
      }
      for (TwillRunResources resources : report.getRunnableResources(runnable)) {
        actual = resources.getLogLevels().get(Logger.ROOT_LOGGER_NAME);
        actualArgs = resources.getLogLevels();
        if (actual != null && actual.equals(expected)) {
          stopped = true;
          break;
        }
      }
      TimeUnit.MILLISECONDS.sleep(100);
    } while (!stopped && stopwatch.elapsedTime(timeoutUnit) < timeout);
    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expectedArgs, actualArgs);
  }
}
