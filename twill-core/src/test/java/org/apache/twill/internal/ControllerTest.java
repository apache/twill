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
package org.apache.twill.internal;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.Command;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.logging.LogHandler;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.KeeperException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 *
 */
public class ControllerTest {

  private static final Logger LOG = LoggerFactory.getLogger(ControllerTest.class);

  @Test
  public void testController() throws ExecutionException, InterruptedException, TimeoutException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    LOG.info("ZKServer: " + zkServer.getConnectionStr());

    try {
      RunId runId = RunIds.generate();
      ZKClientService zkClientService = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      zkClientService.startAndWait();

      Service service = createService(zkClientService, runId);
      service.startAndWait();

      TwillController controller = getController(zkClientService, "testController", runId);
      controller.sendCommand(Command.Builder.of("test").build()).get(2, TimeUnit.SECONDS);
      controller.terminate().get(2, TimeUnit.SECONDS);

      final CountDownLatch terminateLatch = new CountDownLatch(1);
      service.addListener(new ServiceListenerAdapter() {
        @Override
        public void terminated(Service.State from) {
          terminateLatch.countDown();
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      Assert.assertTrue(service.state() == Service.State.TERMINATED || terminateLatch.await(2, TimeUnit.SECONDS));

      zkClientService.stopAndWait();

    } finally {
      zkServer.stopAndWait();
    }
  }

  // Test controller created before service starts.
  @Test
  public void testControllerBefore() throws InterruptedException, ExecutionException, TimeoutException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    LOG.info("ZKServer: " + zkServer.getConnectionStr());
    try {
      RunId runId = RunIds.generate();
      ZKClientService zkClientService = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      zkClientService.startAndWait();

      final CountDownLatch runLatch = new CountDownLatch(1);
      TwillController controller = getController(zkClientService, "testControllerBefore", runId);
      controller.onRunning(new Runnable() {
        @Override
        public void run() {
          runLatch.countDown();
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      Service service = createService(zkClientService, runId);
      service.start();

      Assert.assertTrue(runLatch.await(2, TimeUnit.SECONDS));

      try {
        controller.awaitTerminated(2, TimeUnit.SECONDS);
        Assert.fail("Service should not be terminated");
      } catch (TimeoutException e) {
        // Expected
      }

      service.stop();
      controller.awaitTerminated(120, TimeUnit.SECONDS);

    } finally {
      zkServer.stopAndWait();
    }
  }

  // Test controller listener receive first state change without state transition from service
  @Test
  public void testControllerListener() throws InterruptedException {
    InMemoryZKServer zkServer = InMemoryZKServer.builder().build();
    zkServer.startAndWait();

    LOG.info("ZKServer: " + zkServer.getConnectionStr());
    try {
      RunId runId = RunIds.generate();
      ZKClientService zkClientService = ZKClientService.Builder.of(zkServer.getConnectionStr()).build();
      zkClientService.startAndWait();

      Service service = createService(zkClientService, runId);
      service.startAndWait();

      final CountDownLatch runLatch = new CountDownLatch(1);
      TwillController controller = getController(zkClientService, "testControllerListener", runId);
      controller.onRunning(new Runnable() {
        @Override
        public void run() {
          runLatch.countDown();
        }
      }, Threads.SAME_THREAD_EXECUTOR);

      Assert.assertTrue(runLatch.await(2, TimeUnit.SECONDS));

      service.stopAndWait();

      zkClientService.stopAndWait();
    } finally {
      zkServer.stopAndWait();
    }
  }

  private Service createService(ZKClient zkClient, RunId runId) {
    return new AbstractTwillService(zkClient, runId) {

      private final CountDownLatch stopLatch = new CountDownLatch(1);

      @Override
      protected void doStart() throws Exception {
        LOG.info("Start");
      }

      @Override
      protected void doRun() throws Exception {
        stopLatch.await();
      }

      @Override
      protected void doStop() throws Exception {
        LOG.info("Stop");
      }

      @Override
      protected void triggerShutdown() {
        stopLatch.countDown();
      }
    };
  }

  private TwillController getController(ZKClient zkClient, String appName, RunId runId) {
    AbstractTwillController controller = new AbstractTwillController(appName, runId,
                                                                     zkClient, false, ImmutableList.<LogHandler>of()) {

      @Override
      public void kill() {
        // No-op
      }

      @Override
      protected void instanceNodeUpdated(NodeData nodeData) {
        // No-op
      }

      @Override
      protected void instanceNodeFailed(Throwable cause) {
        // Shutdown if the instance node goes away
        if (cause instanceof KeeperException.NoNodeException) {
          forceShutDown();
        }
      }

      @Override
      public ResourceReport getResourceReport() {
        return null;
      }
    };
    controller.startAndWait();
    return controller;
  }
}
