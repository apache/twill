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

import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import com.google.common.base.Charsets;
import com.google.common.io.LineReader;
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
 * This test is executed by {@link YarnTestSuite}.
 */
public class EchoServerTestRun {

  private static final Logger LOG = LoggerFactory.getLogger(EchoServerTestRun.class);

  @Test
  public void testEchoServer() throws InterruptedException, ExecutionException, IOException,
    URISyntaxException, TimeoutException {
    TwillRunner runner = YarnTestSuite.getTwillRunner();

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
    controller.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        running.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(running.await(30, TimeUnit.SECONDS));

    Iterable<Discoverable> echoServices = controller.discoverService("echo");
    Assert.assertTrue(YarnTestSuite.waitForSize(echoServices, 2, 60));

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
    Assert.assertTrue(YarnTestSuite.waitForSize(echoServices, 3, 60));

    echoServices = controller.discoverService("echo2");

    // Decrease number of instances
    controller.changeInstances("EchoServer", 1);
    Assert.assertTrue(YarnTestSuite.waitForSize(echoServices, 1, 60));

    // Increase number of instances again
    controller.changeInstances("EchoServer", 2);
    Assert.assertTrue(YarnTestSuite.waitForSize(echoServices, 2, 60));

    // Make sure still only one app is running
    Iterable<TwillRunner.LiveInfo> apps = runner.lookupLive();
    Assert.assertTrue(YarnTestSuite.waitForSize(apps, 1, 60));

    // Creates a new runner service to check it can regain control over running app.
    TwillRunnerService runnerService = YarnTestSuite.createTwillRunnerService();
    runnerService.startAndWait();

    try {
      Iterable <TwillController> controllers = runnerService.lookup("EchoServer");
      Assert.assertTrue(YarnTestSuite.waitForSize(controllers, 1, 60));

      for (TwillController c : controllers) {
        LOG.info("Stopping application: " + c.getRunId());
        c.stop().get(30, TimeUnit.SECONDS);
      }

      Assert.assertTrue(YarnTestSuite.waitForSize(apps, 0, 60));
    } finally {
      runnerService.stopAndWait();
    }

    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }
}
