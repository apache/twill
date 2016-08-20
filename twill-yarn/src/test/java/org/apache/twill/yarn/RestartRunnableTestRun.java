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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.JdkFutureAdapters;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.Command;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test restarting of Twill Runnables.
 */
public class RestartRunnableTestRun extends BaseYarnTest {
  private static final Logger LOG = LoggerFactory.getLogger(RestartRunnableTestRun.class);
  private static final String HANGING_RUNNABLE = HangingRunnable.class.getSimpleName();
  private static final String STOPPING_RUNNABLE = StoppingRunnable.class.getSimpleName();
  private static final String HANGING_RUNNABLE_STOP_SECS = "hanging.runnable.stop.secs";

  /**
   * Command that can be sent to HangingRunnable to make it sleep while stopping.
   */
  private static class SleepCommand implements Command {
    private final int sleepTime;

    public SleepCommand(int sleepTime) {
      this.sleepTime = sleepTime;
    }

    @Override
    public String getCommand() {
      return HANGING_RUNNABLE_STOP_SECS;
    }

    @Override
    public Map<String, String> getOptions() {
      return ImmutableMap.of(HANGING_RUNNABLE_STOP_SECS, Integer.toString(sleepTime));
    }
  }

  /**
   * This runnable hangs when it gets a stop message.
   */
  public static final class HangingRunnable extends AbstractTwillRunnable {
    private volatile Thread runThread;
    // Send SleepCommand to update sleepTime to simulate hanging
    private final AtomicInteger sleepTime = new AtomicInteger(1);

    @Override
    public void run() {
      this.runThread = Thread.currentThread();
      LOG.info("Starting Runnable {}", HANGING_RUNNABLE);
      while (!Thread.interrupted()) {
        try {
          TimeUnit.MILLISECONDS.sleep(200);
        } catch (InterruptedException e) {
          break;
        }
      }
      LOG.info("Stopping Runnable {}", HANGING_RUNNABLE);
    }

    @Override
    public void stop() {
      // Do not stop the thread until sleepTime to simulate hanging of the runnable.
      LOG.info("Using sleep time = {} secs", sleepTime);
      try {
        TimeUnit.SECONDS.sleep(sleepTime.get());
      } catch (InterruptedException e) {
        LOG.error("Got exception: ", e);
      }
      if (runThread != null) {
        runThread.interrupt();
      }
    }

    @Override
    public void handleCommand(Command command) throws Exception {
      super.handleCommand(command);
      if (HANGING_RUNNABLE_STOP_SECS.equals(command.getCommand())) {
        int time = Integer.parseInt(command.getOptions().get(HANGING_RUNNABLE_STOP_SECS));
        LOG.info("Got sleep time from message = {} secs", time);
        sleepTime.set(time);
      }
    }
  }

  /**
   * This runnable stops immediately when it gets a stop message.
   */
  public static final class StoppingRunnable extends AbstractTwillRunnable {
    private volatile Thread runThread;

    @Override
    public void run() {
      this.runThread = Thread.currentThread();
      LOG.info("Starting Runnable {}", STOPPING_RUNNABLE);
      while (!Thread.interrupted()) {
        try {
          TimeUnit.MILLISECONDS.sleep(200);
        } catch (InterruptedException e) {
          break;
        }
      }
      LOG.info("Stopping Runnable {}", STOPPING_RUNNABLE);
    }

    @Override
    public void stop() {
      // Interrupt the thread to stop the runnable
      if (runThread != null) {
        runThread.interrupt();
      }
    }
  }

  /**
   * A test TwillApplication to test restarting runnables.
   */
  public static final class RestartTestApplication implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName(RestartTestApplication.class.getSimpleName())
        .withRunnable()
        .add(HANGING_RUNNABLE, new HangingRunnable()).noLocalFiles()
        .add(STOPPING_RUNNABLE, new StoppingRunnable()).noLocalFiles()
        .withOrder()
        .begin(HANGING_RUNNABLE)
        .nextWhenStarted(STOPPING_RUNNABLE)
        .build();
    }
  }

  /**
   * A test TwillApplication with a single runnable.
   */
  public static final class SingleRunnableApp implements TwillApplication {

    @Override
    public TwillSpecification configure() {
      return TwillSpecification.Builder.with()
        .setName(RestartTestApplication.class.getSimpleName())
        .withRunnable()
        .add(HANGING_RUNNABLE, new HangingRunnable()).noLocalFiles()
        .anyOrder()
        .build();
    }
  }

  @Test
  public void testRestartSingleRunnable() throws Exception {
    YarnTwillRunnerService runner = getTwillRunner();
    runner.start();

    LOG.info("Starting application {}", SingleRunnableApp.class.getSimpleName());
    TwillController controller = runner.prepare(new SingleRunnableApp())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
      .start();

    // Lets wait until all runnables have started
    waitForInstance(controller, HANGING_RUNNABLE, "002", 120, TimeUnit.SECONDS);
    waitForContainers(controller, 2, 60, TimeUnit.SECONDS);

    // Now restart runnable
    LOG.info("Restarting runnable {}", HANGING_RUNNABLE);
    controller.restartAllInstances(HANGING_RUNNABLE);
    waitForInstance(controller, HANGING_RUNNABLE, "003", 120, TimeUnit.SECONDS);
    waitForContainers(controller, 2, 60, TimeUnit.SECONDS);

    // Send command to HANGING_RUNNABLE to hang when stopped
    controller.sendCommand(HANGING_RUNNABLE, new SleepCommand(1000)).get();
    LOG.info("Restarting runnable {}", HANGING_RUNNABLE);
    controller.restartAllInstances(HANGING_RUNNABLE);
    waitForInstance(controller, HANGING_RUNNABLE, "004", 120, TimeUnit.SECONDS);
    waitForContainers(controller, 2, 60, TimeUnit.SECONDS);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testRestartRunnable() throws Exception {
    YarnTwillRunnerService runner = getTwillRunner();
    runner.start();

    LOG.info("Starting application {}", RestartTestApplication.class.getSimpleName());
    TwillController controller = runner.prepare(new RestartTestApplication())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out)))
      .start();

    // Lets wait until all runnables have started
    waitForInstance(controller, HANGING_RUNNABLE, "002", 120, TimeUnit.SECONDS);
    waitForInstance(controller, STOPPING_RUNNABLE, "003", 120, TimeUnit.SECONDS);
    waitForContainers(controller, 3, 60, TimeUnit.SECONDS);

    // Send command to first instance of HANGING_RUNNABLE to hang when stopped
    controller.sendCommand(HANGING_RUNNABLE, new SleepCommand(1000)).get();

    // Increase instances of both runnables
    LOG.info("Increasing instances of both runnables");
    allAsList(controller.changeInstances(HANGING_RUNNABLE, 3),
              controller.changeInstances(STOPPING_RUNNABLE, 2)
    ).get(120, TimeUnit.SECONDS);
    waitForInstance(controller, HANGING_RUNNABLE, "005", 120, TimeUnit.SECONDS); // +2 containers
    waitForInstance(controller, STOPPING_RUNNABLE, "006", 120, TimeUnit.SECONDS); // +1 container
    waitForContainers(controller, 6, 60, TimeUnit.SECONDS);

    // Now restart both runnables (002 instance of HANGING_RUNNABLE will be killed)
    LOG.info("Restarting all instances of runnables {} and {}", HANGING_RUNNABLE, STOPPING_RUNNABLE);
    allAsList(
      controller.restartAllInstances(HANGING_RUNNABLE),
      controller.restartAllInstances(STOPPING_RUNNABLE)
    ).get(120, TimeUnit.SECONDS);
    waitForInstance(controller, HANGING_RUNNABLE, "009", 120, TimeUnit.SECONDS);
    waitForInstance(controller, STOPPING_RUNNABLE, "011", 120, TimeUnit.SECONDS);
    waitForContainers(controller, 6, 60, TimeUnit.SECONDS);

    // Restart a single runnable from both
    LOG.info("Restarting a single runnable of both");
    allAsList(
      controller.restartInstances(HANGING_RUNNABLE, 1),
      controller.restartInstances(ImmutableMap.of(STOPPING_RUNNABLE, Collections.singleton(0)))
    ).get(120, TimeUnit.SECONDS);
    waitForInstance(controller, HANGING_RUNNABLE, "012", 120, TimeUnit.SECONDS);
    waitForInstance(controller, STOPPING_RUNNABLE, "013", 120, TimeUnit.SECONDS);
    waitForContainers(controller, 6, 60, TimeUnit.SECONDS);

    // Send command to all instances of HANGING_RUNNABLE to wait for 10 seconds when stopped
    controller.sendCommand(HANGING_RUNNABLE, new SleepCommand(10)).get();

    // Reduce instances of both runnables to 1
    LOG.info("Decreasing instances of both runnables");
    allAsList(controller.changeInstances(HANGING_RUNNABLE, 1),
              controller.changeInstances(STOPPING_RUNNABLE, 1)
    ).get(120, TimeUnit.SECONDS);
    waitForInstance(controller, HANGING_RUNNABLE, "007", 120, TimeUnit.SECONDS);
    waitForInstance(controller, STOPPING_RUNNABLE, "013", 120, TimeUnit.SECONDS); // instance 0 is 013 due to restart
    waitForContainers(controller, 3, 60, TimeUnit.SECONDS);

    LOG.info("Stopping application {}", RestartTestApplication.class.getSimpleName());
    controller.terminate().get(120, TimeUnit.SECONDS);

    // Sleep a bit for full cleanup
    TimeUnit.SECONDS.sleep(2);
  }

  private void waitForContainers(TwillController controller, int count, long timeout, TimeUnit timeoutUnit)
    throws Exception {
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    int yarnContainers = 0;
    int twillContainers = 0;
    do {
      if (controller.getResourceReport() != null) {
        yarnContainers =
          getApplicationResourceReport(controller.getResourceReport().getApplicationId()).getNumUsedContainers();
        twillContainers = getTwillContainersUsed(controller);
        if (yarnContainers == count && twillContainers == count) {
          return;
        }
      }
      TimeUnit.SECONDS.sleep(1);
    } while (stopwatch.elapsedTime(timeoutUnit) < timeout);

    throw new TimeoutException("Timeout reached while waiting for num containers to be " +  count +
                                 ". Yarn containers = " + yarnContainers + ", Twill containers = " + twillContainers);
  }

  private void waitForInstance(TwillController controller, String runnable, String yarnInstanceId,
                               long timeout, TimeUnit timeoutUnit) throws InterruptedException, TimeoutException {
    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    do {
      ResourceReport report = controller.getResourceReport();
      if (report != null && report.getRunnableResources(runnable) != null) {
        for (TwillRunResources resources : report.getRunnableResources(runnable)) {
          if (resources.getContainerId().endsWith(yarnInstanceId)) {
            return;
          }
        }
      }
      TimeUnit.SECONDS.sleep(1);
    } while (stopwatch.elapsedTime(timeoutUnit) < timeout);

    throw new TimeoutException("Timeout reached while waiting for runnable " +
                                 runnable + " instance " + yarnInstanceId);
  }

  private int getTwillContainersUsed(TwillController controller) {
    if (controller.getResourceReport() == null) {
      return 0;
    }

    int count = 1; // 1 for app master container
    ResourceReport resourceReport = controller.getResourceReport();
    for (Collection<TwillRunResources> resources : resourceReport.getResources().values()) {
      count += resources.size();
    }
    return count;
  }

  @SafeVarargs
  private final <V> ListenableFuture<List<V>> allAsList(Future<? extends V>... futures) {
    ImmutableList.Builder<ListenableFuture<? extends V>> listBuilder = ImmutableList.builder();
    for (Future<? extends V> future : futures) {
      listBuilder.add(JdkFutureAdapters.listenInPoolThread(future));
    }
    return Futures.allAsList(listBuilder.build());
  }
}
