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
package org.apache.twill.test;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.yarn.BaseYarnTest;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Unit test for running twill under Java8.
 */
public class Java8Test extends BaseYarnTest {

  @Test
  public void test() throws ExecutionException, InterruptedException, TimeoutException {
    TwillRunner runner = getTwillRunner();

    // Start the TestRunnable and make sure it is executed with the log message emitted.
    CountDownLatch logLatch = new CountDownLatch(1);
    TwillController controller = runner.prepare(new TestRunnable())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .addLogHandler(logEntry -> {
        if ("Hello World".equals(logEntry.getMessage())) {
          logLatch.countDown();
        }
      })
      .start();

    Assert.assertTrue(logLatch.await(120, TimeUnit.SECONDS));
    controller.terminate().get(120, TimeUnit.SECONDS);
  }

  /**
   * A {@link TwillRunnable} that emits a log message and wait to be stopped.
   */
  public static final class TestRunnable extends AbstractTwillRunnable {

    private static final Logger LOG = LoggerFactory.getLogger(TestRunnable.class);

    private final CountDownLatch stopLatch = new CountDownLatch(1);

    @Override
    public void run() {
      LOG.info(Compute.getMessage());
      Uninterruptibles.awaitUninterruptibly(stopLatch);
    }

    @Override
    public void stop() {
      stopLatch.countDown();
    }
  }

  /**
   * An interface with static method, which is a Java 8 feature.
   */
  public interface Compute {
    static String getMessage() {
      return "Hello World";
    }
  }
}
