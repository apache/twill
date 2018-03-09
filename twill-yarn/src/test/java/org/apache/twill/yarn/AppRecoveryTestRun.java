/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.twill.yarn;

import com.google.common.io.Files;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.internal.yarn.YarnUtils;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for application master resilience.
 */
public class AppRecoveryTestRun extends BaseYarnTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testAMRestart() throws Exception {
    // Only run it with Hadoop-2.1 or above
    Assume.assumeTrue(YarnUtils.getHadoopVersion().compareTo(YarnUtils.HadoopVersions.HADOOP_21) >= 0);
    // Don't run this test in Mac, as there would be leftover java process (HADOOP-12317)
    // The test can be force to run by turning on the "force-mac-tests" maven profile
    // After the test finished, run the `jps` command and delete all `TwillLauncher` processes
    Assume.assumeTrue(Boolean.parseBoolean(System.getProperty("force.mac.tests")) ||
                      !System.getProperty("os.name").toLowerCase().contains("mac"));

    File watchFile = TEMP_FOLDER.newFile();
    watchFile.delete();

    // Start the testing app, and wait for 4 log lines that match the pattern emitted by the event handler (AM)
    // and from the runnable
    final Semaphore semaphore = new Semaphore(0);
    TwillRunner runner = getTwillRunner();
    TwillController controller = runner.prepare(new TestApp(new TestEventHandler(watchFile)))
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      // Use a log handler to match messages from AM and the Runnable to make sure the log collection get resumed
      // correctly after the AM restarted
      .addLogHandler(new LogHandler() {
        @Override
        public void onLog(LogEntry logEntry) {
          String message = logEntry.getMessage();
          if (message.equals("Container for " + TestRunnable.class.getSimpleName() + " launched")) {
            semaphore.release();
          } else if (message.equals("Running 0")) {
            semaphore.release();
          }
        }
      })
      .start();

    // Wait for the first attempt running
    Assert.assertTrue(semaphore.tryAcquire(2, 2, TimeUnit.MINUTES));
    // Touch the watchFile so that the event handler will kill the AM
    Files.touch(watchFile);
    // Wait for the second attempt running
    Assert.assertTrue(semaphore.tryAcquire(2, 2, TimeUnit.MINUTES));

    controller.terminate().get();
  }

  /**
   * A {@link EventHandler} for killing the first attempt of the application.
   */
  public static final class TestEventHandler extends EventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TestEventHandler.class);

    private File watchFile;

    TestEventHandler(File watchFile) {
      this.watchFile = watchFile;
    }

    @Override
    public void containerLaunched(String runnableName, int instanceId, String containerId) {
      LOG.info("Container for {} launched", runnableName);

      if (containerId.contains("_01_")) {
        final File watchFile = new File(context.getSpecification().getConfigs().get("watchFile"));
        Thread t = new Thread() {
          @Override
          public void run() {
            // Wait for the watch file to be available, then kill the process
            while (!watchFile.exists()) {
              Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
            Runtime.getRuntime().halt(-1);
          }
        };
        t.setDaemon(true);
        t.start();
      }
    }

    @Override
    protected Map<String, String> getConfigs() {
      return Collections.singletonMap("watchFile", watchFile.getAbsolutePath());
    }
  }

  /**
   * Application for testing
   */
  public static final class TestApp implements TwillApplication {

    private final EventHandler eventHandler;

    public TestApp(EventHandler eventHandler) {
      this.eventHandler = eventHandler;
    }

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("TestApp")
        .withRunnable()
        .add(new TestRunnable()).noLocalFiles()
        .anyOrder()
        .withEventHandler(eventHandler).build();
    }
  }

  /**
   * Runnable for testing
   */
  public static final class TestRunnable extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(TestRunnable.class);

    private final CountDownLatch stopLatch = new CountDownLatch(1);

    @Override
    public void run() {
      long count = 0;
      try {
        while (!stopLatch.await(2, TimeUnit.SECONDS)) {
          LOG.info("Running {}", count++);
        }
      } catch (InterruptedException e) {
        LOG.info("Interrupted", e);
      }
    }

    @Override
    public void stop() {
      stopLatch.countDown();
    }
  }
}
