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

package org.apache.twill.yarn;

import com.google.common.collect.ImmutableMap;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;

import java.io.PrintWriter;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Test for requesting different container size in different order.
 * It specifically test for workaround for YARN-314.
 */
public class ContainerSizeTestRun extends BaseYarnTest {

  @Test
  public void testContainerSize() throws InterruptedException, TimeoutException, ExecutionException {
    TwillRunner runner = YarnTestUtils.getTwillRunner();
    TwillController controller = runner.prepare(new SleepApp())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();

    try {
      ServiceDiscovered discovered = controller.discoverService("sleep");
      Assert.assertTrue(YarnTestUtils.waitForSize(discovered, 2, 120));
    } finally {
      controller.terminate().get(120, TimeUnit.SECONDS);
    }
  }


  /**
   * An application that has two runnables with different memory size.
   */
  public static final class SleepApp implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      ResourceSpecification largeRes = ResourceSpecification.Builder.with()
        .setVirtualCores(1)
        .setMemory(1024, ResourceSpecification.SizeUnit.MEGA)
        .build();

      ResourceSpecification smallRes = ResourceSpecification.Builder.with()
        .setVirtualCores(1)
        .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
        .build();

      return TwillSpecification.Builder.with()
        .setName("SleepApp")
        .withRunnable()
          .add("sleep1", new SleepRunnable(12345), largeRes).noLocalFiles()
          .add("sleep2", new SleepRunnable(12346), smallRes).noLocalFiles()
        .withOrder()
          .begin("sleep1")
          .nextWhenStarted("sleep2")
        .build();
    }
  }


  /**
   * A runnable that sleep for 120 seconds.
   */
  public static final class SleepRunnable extends AbstractTwillRunnable {

    private volatile Thread runThread;

    public SleepRunnable(int port) {
      super(ImmutableMap.of("port", Integer.toString(port)));
    }

    @Override
    public void run() {
      runThread = Thread.currentThread();
      getContext().announce("sleep", Integer.parseInt(getContext().getSpecification().getConfigs().get("port")));
      try {
        TimeUnit.SECONDS.sleep(120);
      } catch (InterruptedException e) {
        // Ignore.
      }
    }

    @Override
    public void stop() {
      if (runThread != null) {
        runThread.interrupt();
      }
    }
  }
}
