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
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Services;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Test for LogHandler able to receive logs from AM and runnable.
 */
public class LogHandlerTestRun extends BaseYarnTest {

  @Test
  public void testLogHandler() throws ExecutionException, InterruptedException {
    final CountDownLatch latch = new CountDownLatch(2);

    LogHandler logHandler = new LogHandler() {
      @Override
      public void onLog(LogEntry logEntry) {
        // Would expect logs from AM and the runnable.
        if (logEntry.getMessage().startsWith("Starting runnable " + LogRunnable.class.getSimpleName())) {
          latch.countDown();
        } else if (logEntry.getMessage().equals("Running")) {
          latch.countDown();
        }
      }
    };

    TwillRunner runner = YarnTestUtils.getTwillRunner();
    TwillController controller = runner.prepare(new LogRunnable())
                                       .addLogHandler(logHandler)
                                       .start();

    Services.getCompletionFuture(controller).get();
    latch.await(1, TimeUnit.SECONDS);
  }

  /**
   * TwillRunnable for the test case to simply emit one log line.
   */
  public static final class LogRunnable extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(LogRunnable.class);


    @Override
    public void run() {
      LOG.info("Running");
    }

    @Override
    public void stop() {

    }
  }
}
