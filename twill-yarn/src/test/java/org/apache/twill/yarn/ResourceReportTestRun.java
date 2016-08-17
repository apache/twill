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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.LineReader;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.internal.EnvKeys;
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
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Using echo server to test resource reports.
 */
public final class ResourceReportTestRun extends BaseYarnTest {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceReportTestRun.class);

  private class ResourceApplication implements TwillApplication {
    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName("ResourceApplication")
        .withRunnable()
          .add("echo1", new EchoServer(), ResourceSpecification.Builder.with()
            .setVirtualCores(1)
            .setMemory(256, ResourceSpecification.SizeUnit.MEGA)
            .setInstances(2).build()).noLocalFiles()
          .add("echo2", new EchoServer(), ResourceSpecification.Builder.with()
            .setVirtualCores(2)
            .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
            .setInstances(1).build()).noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  @Test
  public void testRunnablesGetAllowedResourcesInEnv() throws InterruptedException, IOException,
    TimeoutException, ExecutionException {
    TwillRunner runner = getTwillRunner();

    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(2048, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(1)
      .build();
    TwillController controller = runner.prepare(new EnvironmentEchoServer(), resourceSpec)
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .withApplicationArguments("envecho")
      .withArguments("EnvironmentEchoServer", "echo2")
      .start();

    final CountDownLatch running = new CountDownLatch(1);
    controller.onRunning(new Runnable() {
      @Override
      public void run() {
        running.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(running.await(120, TimeUnit.SECONDS));

    Iterable<Discoverable> envEchoServices = controller.discoverService("envecho");
    Assert.assertTrue(waitForSize(envEchoServices, 1, 120));

    // TODO: check virtual cores once yarn adds the ability
    Map<String, String> expectedValues = Maps.newHashMap();
    expectedValues.put(EnvKeys.YARN_CONTAINER_MEMORY_MB, "2048");
    expectedValues.put(EnvKeys.TWILL_INSTANCE_COUNT, "1");

    // check environment of the runnable.
    Discoverable discoverable = envEchoServices.iterator().next();
    for (Map.Entry<String, String> expected : expectedValues.entrySet()) {
      try (
        Socket socket = new Socket(discoverable.getSocketAddress().getHostName(),
                                   discoverable.getSocketAddress().getPort())
      ) {
        PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8), true);
        LineReader reader = new LineReader(new InputStreamReader(socket.getInputStream(), Charsets.UTF_8));
        writer.println(expected.getKey());
        Assert.assertEquals(expected.getValue(), reader.readLine());
      }
    }

    controller.terminate().get(120, TimeUnit.SECONDS);
    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }

  @Test
  public void testResourceReportWithFailingContainers() throws InterruptedException, IOException,
    TimeoutException, ExecutionException {
    TwillRunner runner = getTwillRunner();

    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(256, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(2)
      .build();
    TwillController controller = runner.prepare(new BuggyServer(), resourceSpec)
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .withApplicationArguments("echo")
      .withArguments("BuggyServer", "echo2")
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
    // check that we have 2 runnables.
    ResourceReport report = controller.getResourceReport();
    Assert.assertEquals(2, report.getRunnableResources("BuggyServer").size());

    // cause a divide by 0 in one server
    Discoverable discoverable = echoServices.iterator().next();
    try (
      Socket socket = new Socket(discoverable.getSocketAddress().getAddress(),
                                 discoverable.getSocketAddress().getPort())
    ) {
      PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), Charsets.UTF_8), true);
      writer.println("0");
    }

    // takes some time for app master to find out the container completed...
    int count = 0;
    while (count < 100) {
      report = controller.getResourceReport();
      // check that we have 1 runnable, not 2.
      if (report.getRunnableResources("BuggyServer").size() == 1) {
        break;
      }
      LOG.info("Wait for BuggyServer to have 1 instance left. Trial {}.", count);
      count++;
      TimeUnit.SECONDS.sleep(1);
    }
    Assert.assertTrue("Still has 2 contains running after 100 seconds", count < 100);

    controller.terminate().get(100, TimeUnit.SECONDS);
    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }

  @Test
  public void testResourceReport() throws InterruptedException, ExecutionException, IOException,
    URISyntaxException, TimeoutException {
    TwillRunner runner = getTwillRunner();

    final TwillController controller = runner.prepare(new ResourceApplication())
                                        .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                                        .withApplicationArguments("echo")
                                        .withArguments("echo1", "echo1")
                                        .withArguments("echo2", "echo2")
                                        .start();

    final CountDownLatch running = new CountDownLatch(1);
    controller.onRunning(new Runnable() {
      @Override
      public void run() {
        running.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Assert.assertTrue(running.await(120, TimeUnit.SECONDS));

    // wait for 3 echo servers to come up
    Iterable<Discoverable> echoServices = controller.discoverService("echo");
    Assert.assertTrue(waitForSize(echoServices, 3, 120));
    ResourceReport report = controller.getResourceReport();
    // make sure resources for echo1 and echo2 are there
    Map<String, Collection<TwillRunResources>> usedResources = report.getResources();
    Assert.assertEquals(2, usedResources.keySet().size());
    Assert.assertTrue(usedResources.containsKey("echo1"));
    Assert.assertTrue(usedResources.containsKey("echo2"));

    waitForSize(new Iterable<String>() {
      @Override
      public Iterator<String> iterator() {
        return controller.getResourceReport().getServices().iterator();
      }
    }, 3, 120);
    report = controller.getResourceReport();
    Assert.assertEquals(ImmutableSet.of("echo", "echo1", "echo2"), ImmutableSet.copyOf(report.getServices()));

    Collection<TwillRunResources> echo1Resources = usedResources.get("echo1");
    // 2 instances of echo1
    Assert.assertEquals(2, echo1Resources.size());
    // TODO: check cores after hadoop-2.1.0
    for (TwillRunResources resources : echo1Resources) {
      Assert.assertEquals(256, resources.getMemoryMB());
    }

    Collection<TwillRunResources> echo2Resources = usedResources.get("echo2");
    // 2 instances of echo1
    Assert.assertEquals(1, echo2Resources.size());
    // TODO: check cores after hadoop-2.1.0
    for (TwillRunResources resources : echo2Resources) {
      Assert.assertEquals(512, resources.getMemoryMB());
    }

    // Decrease number of instances of echo1 from 2 to 1
    controller.changeInstances("echo1", 1).get(60, TimeUnit.SECONDS);
    echoServices = controller.discoverService("echo1");
    Assert.assertTrue(waitForSize(echoServices, 1, 60));
    report = controller.getResourceReport();

    // make sure resources for echo1 and echo2 are there
    usedResources = report.getResources();
    Assert.assertEquals(2, usedResources.keySet().size());
    Assert.assertTrue(usedResources.containsKey("echo1"));
    Assert.assertTrue(usedResources.containsKey("echo2"));

    echo1Resources = usedResources.get("echo1");
    // 1 instance of echo1 now
    Assert.assertEquals(1, echo1Resources.size());
    // TODO: check cores after hadoop-2.1.0
    for (TwillRunResources resources : echo1Resources) {
      Assert.assertEquals(256, resources.getMemoryMB());
    }

    echo2Resources = usedResources.get("echo2");
    // 2 instances of echo1
    Assert.assertEquals(1, echo2Resources.size());
    // TODO: check cores after hadoop-2.1.0
    for (TwillRunResources resources : echo2Resources) {
      Assert.assertEquals(512, resources.getMemoryMB());
    }

    controller.terminate().get(120, TimeUnit.SECONDS);
    // Sleep a bit before exiting.
    TimeUnit.SECONDS.sleep(2);
  }
}
