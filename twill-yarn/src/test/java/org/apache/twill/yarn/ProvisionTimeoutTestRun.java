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
import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.EventHandlerContext;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.junit.Test;

import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public final class ProvisionTimeoutTestRun extends BaseYarnTest {

  @Test
  public void testProvisionTimeout() throws InterruptedException, ExecutionException, TimeoutException {
    TwillRunner runner = getTwillRunner();

    TwillController controller = runner.prepare(new TimeoutApplication())
                                       .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                                       .start();

    // The provision should failed in 30 seconds after AM started, which AM could took a while to start.
    // Hence we give 90 seconds max time here.
    try {
      controller.awaitTerminated(90, TimeUnit.SECONDS);
    } finally {
      // If it timeout, kill the app as cleanup.
      controller.kill();
    }
  }

  /**
   * The handler for testing timeout handling.
   */
  public static final class Handler extends EventHandler {

    private boolean abort;

    @Override
    protected Map<String, String> getConfigs() {
      return ImmutableMap.of("abort", "true");
    }

    @Override
    public void initialize(EventHandlerContext context) {
      this.abort = Boolean.parseBoolean(context.getSpecification().getConfigs().get("abort"));
    }

    @Override
    public TimeoutAction launchTimeout(Iterable<TimeoutEvent> timeoutEvents) {
      if (abort) {
        return TimeoutAction.abort();
      } else {
        return TimeoutAction.recheck(10, TimeUnit.SECONDS);
      }
    }
  }

  /**
   * Testing application for timeout.
   */
  public static final class TimeoutApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("TimeoutApplication")
        .withRunnable()
        .add(new TimeoutRunnable(),
             ResourceSpecification.Builder.with()
               .setVirtualCores(1)
               .setMemory(8, ResourceSpecification.SizeUnit.GIGA).build())
        .noLocalFiles()
        .anyOrder()
        .withEventHandler(new Handler())
        .build();
    }
  }

  /**
   * A runnable that do nothing, as it's not expected to get provisioned.
   */
  public static final class TimeoutRunnable extends AbstractTwillRunnable {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void stop() {
      latch.countDown();
    }

    @Override
    public void run() {
      // Simply block here
      try {
        latch.await();
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
