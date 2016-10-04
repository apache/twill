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
import com.google.common.base.Stopwatch;
import com.google.common.collect.Maps;
import com.google.common.io.LineReader;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Using echo server to test various behavior of YarnTwillService.
 */
public final class EchoServerTestRun extends BaseYarnTest {

  private static final Logger LOG = LoggerFactory.getLogger(EchoServerTestRun.class);

  @Test
  public void testEchoServer() throws Exception {
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

      try (
        Socket socket = new Socket(discoverable.getSocketAddress().getAddress(),
                                   discoverable.getSocketAddress().getPort())
      ) {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8), true);
        LineReader reader = new LineReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));

        writer.println(msg);
        Assert.assertEquals(msg, reader.readLine());
      }
    }

    // Increase number of instances
    controller.changeInstances("EchoServer", 3).get(60, TimeUnit.SECONDS);
    Assert.assertTrue(waitForSize(echoServices, 3, 120));

    echoServices = controller.discoverService("echo2");

    // Decrease number of instances
    controller.changeInstances("EchoServer", 1).get(60, TimeUnit.SECONDS);
    Assert.assertTrue(waitForSize(echoServices, 1, 120));

    // Increase number of instances again
    controller.changeInstances("EchoServer", 2).get(60, TimeUnit.SECONDS);
    Assert.assertTrue(waitForSize(echoServices, 2, 120));

    // Test restart on instances for runnable
    Map<Integer, String> instanceIdToContainerId = Maps.newHashMap();
    ResourceReport report = waitForAfterRestartResourceReport(controller, "EchoServer", 15L,
                                                              TimeUnit.MINUTES, 2, null);
    Assert.assertTrue(report != null);
    Collection<TwillRunResources> runResources = report.getRunnableResources("EchoServer");
    for (TwillRunResources twillRunResources : runResources) {
      instanceIdToContainerId.put(twillRunResources.getInstanceId(), twillRunResources.getContainerId());
    }

    controller.restartAllInstances("EchoServer").get(60, TimeUnit.SECONDS);
    Assert.assertTrue(waitForSize(echoServices, 2, 120));

    report = waitForAfterRestartResourceReport(controller, "EchoServer", 15L, TimeUnit.MINUTES, 2,
                                               instanceIdToContainerId);
    Assert.assertTrue(report != null);

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

  @Test
  public void testZKCleanup() throws Exception {
    final ZKClientService zkClient = ZKClientService.Builder.of(getZKConnectionString() + "/twill").build();
    zkClient.startAndWait();

    try {
      TwillRunner runner = getTwillRunner();

      // Start an application and stop it.
      TwillController controller = runner.prepare(new EchoServer())
        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
        .withApplicationArguments("echo")
        .withArguments("EchoServer", "echo2")
        .start();

      Iterable<Discoverable> echoServices = controller.discoverService("echo");
      Assert.assertTrue(waitForSize(echoServices, 1, 120));

      controller.terminate().get();

      // Verify the ZK node gets cleanup
      waitFor(null, new Callable<Stat>() {
        @Override
        public Stat call() throws Exception {
          return zkClient.exists("/EchoServer").get();
        }
      }, 10000, 100, TimeUnit.MILLISECONDS);

      // Start two instances of the application and stop one of it
      List<TwillController> controllers = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        controllers.add(runner.prepare(new EchoServer())
                          .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                          .withApplicationArguments("echo")
                          .withArguments("EchoServer", "echo2")
                          .start());
      }

      // There should be two instances up and running.
      echoServices = controllers.get(1).discoverService("echo");
      Assert.assertTrue(waitForSize(echoServices, 2, 120));

      // Stop one instance of the app
      controllers.get(0).terminate().get();

      // Verify the ZK node should still be there
      Assert.assertNotNull(zkClient.exists("/EchoServer").get());

      // We should still be able to do discovery, which depends on the ZK node.
      echoServices = controllers.get(1).discoverService("echo");
      Assert.assertTrue(waitForSize(echoServices, 1, 120));

      // Stop second instance of the app
      controllers.get(1).terminate().get();

      // Verify the ZK node gets cleanup
      waitFor(null, new Callable<Stat>() {
        @Override
        public Stat call() throws Exception {
          return zkClient.exists("/EchoServer").get();
        }
      }, 10000, 100, TimeUnit.MILLISECONDS);

    } finally {
      zkClient.stopAndWait();
    }
  }

  /**
   *  Need helper method here to wait for getting resource report because {@link TwillController#getResourceReport()}
   *  could return null if the application has not fully started.
   *
   *  This method helps validate restart scenario.
   *
   *  To avoid long sleep if instanceIdToContainerId is passed, then compare the container ids to ones before.
   *  Otherwise just return the valid resource report.
   */
  @Nullable
  private ResourceReport waitForAfterRestartResourceReport(TwillController controller, String runnable, long timeout,
                                                           TimeUnit timeoutUnit, int numOfResources,
                                                           @Nullable Map<Integer, String> instanceIdToContainerId) {
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    do {
      ResourceReport report = controller.getResourceReport();
      if (report == null || report.getRunnableResources(runnable) == null) {
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } else if (report.getRunnableResources(runnable) == null ||
          report.getRunnableResources(runnable).size() != numOfResources) {
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      } else {
        if (instanceIdToContainerId == null) {
          LOG.info("Return resource report without comparing container ids.");
          return report;
        }
        Collection<TwillRunResources> runResources = report.getRunnableResources(runnable);
        boolean isSameContainer = false;
        for (TwillRunResources twillRunResources : runResources) {
          int instanceId = twillRunResources.getInstanceId();
          if (twillRunResources.getContainerId().equals(instanceIdToContainerId.get(instanceId))) {
            // found same container id lets wait again.
            LOG.warn("Found an instance id {} with same container id {} for restart all, let's wait for a while.",
                     instanceId, twillRunResources.getContainerId());
            isSameContainer = true;
            break;
          }
        }
        if (!isSameContainer) {
          LOG.info("Get set of different container ids for restart.");
          return report;
        }
        Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
      }
    } while (stopwatch.elapsedTime(timeoutUnit) < timeout);

    LOG.error("Unable to get different container ids for restart.");
    return null;
  }
}
