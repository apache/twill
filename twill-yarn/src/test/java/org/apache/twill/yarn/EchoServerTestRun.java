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

import com.google.common.base.Charsets;
import com.google.common.io.LineReader;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.URISyntaxException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Using echo server to test various behavior of YarnTwillService.
 */
public final class EchoServerTestRun extends BaseYarnTest {

  private static final Logger LOG = LoggerFactory.getLogger(EchoServerTestRun.class);

  @Test
  public void testEchoServer() throws InterruptedException, ExecutionException, IOException,
    URISyntaxException, TimeoutException {
    TwillRunner runner = getTwillRunner();

    TwillController controller = runner.prepare(new EchoServer(),
                                                ResourceSpecification.Builder.with()
                                                         .setVirtualCores(1)
                                                         .setMemory(1, ResourceSpecification.SizeUnit.GIGA)
                                                         .setInstances(2)
                                                         .build())
                                        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                                        .withApplicationArguments("echo")
                                        .withArguments("EchoServer", "echo2")
                                        .start();

    final CountDownLatch running = new CountDownLatch(1);
    controller.onRunning(new Runnable() {
      @Override
      public void run() {
        running.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(running.await(120, TimeUnit.SECONDS));

    Iterable<Discoverable> echoServices = controller.discoverService("echo");
    Assert.assertTrue(waitForSize(echoServices, 2, 120));

    for (Discoverable discoverable : echoServices) {
      String msg = "Hello: " + discoverable.getSocketAddress();

      Socket socket = new Socket(discoverable.getSocketAddress().getAddress(),
                                 discoverable.getSocketAddress().getPort());
      try {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8), true);
        LineReader reader = new LineReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));

        writer.println(msg);
        Assert.assertEquals(msg, reader.readLine());
      } finally {
        socket.close();
      }
    }

    // Increase number of instances
    controller.changeInstances("EchoServer", 3);
    Assert.assertTrue(waitForSize(echoServices, 3, 120));

    echoServices = controller.discoverService("echo2");

    // Decrease number of instances
    controller.changeInstances("EchoServer", 1);
    Assert.assertTrue(waitForSize(echoServices, 1, 120));

    // Increase number of instances again
    controller.changeInstances("EchoServer", 2);
    Assert.assertTrue(waitForSize(echoServices, 2, 120));

    // Make sure still only one app is running
    Iterable<TwillRunner.LiveInfo> apps = runner.lookupLive();
    Assert.assertTrue(waitForSize(apps, 1, 120));

    // Creates a new runner service to check it can regain control over running app.
    TwillRunnerService runnerService = TWILL_TESTER.createTwillRunnerService();
    runnerService.start();

    try {
      Iterable <TwillController> controllers = runnerService.lookup("EchoServer");
      Assert.assertTrue(waitForSize(controllers, 1, 120));

      for (TwillController c : controllers) {
        LOG.info("Stopping application: " + c.getRunId());
        c.terminate().get(30, TimeUnit.SECONDS);
      }

      Assert.assertTrue(waitForSize(apps, 0, 120));
    } finally {
      runnerService.stop();
    }

    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }
}
