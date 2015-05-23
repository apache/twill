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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.common.Threads;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ConnectionMXBean;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import javax.management.StringValueExp;

/**
 * Test for testing ZK session expire from AM container.
 */
public class SessionExpireTestRun extends BaseYarnTest {

  private static final Logger LOG = LoggerFactory.getLogger(SessionExpireTestRun.class);

  @Test
  public void testAppSessionExpire() throws InterruptedException, ExecutionException, TimeoutException {
    TwillRunner runner = getTwillRunner();
    TwillController controller = runner.prepare(new SleepRunnable(600))
                                       .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                                       .start();

    final CountDownLatch runLatch = new CountDownLatch(1);
    controller.onRunning(new Runnable() {
      @Override
      public void run() {
        runLatch.countDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    // Wait for application running
    Assert.assertTrue(runLatch.await(60, TimeUnit.SECONDS));

    // Find the app master ZK session and expire it two times, 10 seconds apart.
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(expireAppMasterZKSession(controller, 10, TimeUnit.SECONDS));

      try {
        controller.awaitTerminated(10, TimeUnit.SECONDS);
        Assert.fail("Unexpected application termination.");
      } catch (TimeoutException e) {
        // OK, expected.
      }
    }

    controller.terminate().get(120, TimeUnit.SECONDS);
  }

  private boolean expireAppMasterZKSession(TwillController controller, long timeout, TimeUnit timeoutUnit) {
    MBeanServer mbeanServer = MBeanRegistry.getInstance().getPlatformMBeanServer();
    QueryExp query = Query.isInstanceOf(new StringValueExp(ConnectionMXBean.class.getName()));

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    do {
      // Find the AM session and expire it
      Set<ObjectName> connectionBeans = mbeanServer.queryNames(ObjectName.WILDCARD, query);
      for (ObjectName objectName : connectionBeans) {

        ConnectionMXBean connectionBean = MBeanServerInvocationHandler.newProxyInstance(mbeanServer, objectName,
                                                                                        ConnectionMXBean.class, false);
        for (String node : connectionBean.getEphemeralNodes()) {
          if (node.endsWith("/instances/" + controller.getRunId().getId())) {
            // This is the AM, expire the session.
            LOG.info("Kill AM session {}", connectionBean.getSessionId());
            connectionBean.terminateSession();
            return true;
          }
        }
      }
      Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
    } while (stopwatch.elapsedTime(timeoutUnit) < timeout);

    return false;
  }

  /**
   * A TwillRunnable that just keep sleeping.
   */
  public static final class SleepRunnable extends AbstractTwillRunnable {

    private Thread runThread;

    public SleepRunnable(long sleepSeconds) {
      super(ImmutableMap.of("sleepSeconds", Long.toString(sleepSeconds)));
    }

    @Override
    public void stop() {
      if (runThread != null) {
        runThread.interrupt();
      }
    }

    @Override
    public void run() {
      try {
        runThread = Thread.currentThread();
        TimeUnit.SECONDS.sleep(Long.parseLong(getArgument("sleepSeconds")));
      } catch (InterruptedException e) {
        // Ignore
      }
    }
  }
}
