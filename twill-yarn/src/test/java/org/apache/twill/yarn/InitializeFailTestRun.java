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
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Services;
import org.junit.Test;

import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for no retry on runnable initialize failure.
 */
public class InitializeFailTestRun extends BaseYarnTest {

  @Test
  public void testInitFail() throws InterruptedException, ExecutionException, TimeoutException {
    TwillRunner runner = YarnTestUtils.getTwillRunner();
    TwillController controller = runner.prepare(new InitFailRunnable())
                                       .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
                                       .start();

    Services.getCompletionFuture(controller).get(2, TimeUnit.MINUTES);
  }

  /**
   * TwillRunnable class that throws exception in initialize.
   */
  public static final class InitFailRunnable extends AbstractTwillRunnable {

    @Override
    public void initialize(TwillContext context) {
      throw new IllegalStateException("Fail to init");
    }

    @Override
    public void run() {
      // No-op
    }

    @Override
    public void stop() {
      // No-op
    }
  }
}
