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
import org.apache.twill.api.TwillController;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.PrinterLogHandler;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
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

  @Test
  public void testDebugLogLevel()throws Exception {
    YarnTwillRunnerService runner = getTwillRunner();
    runner.start();

    TwillController controller = runner.prepare(new LogLevelTestRunnable())
      .setLogLevel(LogEntry.Level.DEBUG)
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
      .start();

    controller.terminate().get(120, TimeUnit.SECONDS);
  }

}
