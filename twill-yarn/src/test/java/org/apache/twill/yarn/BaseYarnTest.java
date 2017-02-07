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
import com.google.common.collect.Iterables;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.twill.api.Configs;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for all YARN tests.
 */
public abstract class BaseYarnTest {

  private static final Logger LOG = LoggerFactory.getLogger(BaseYarnTest.class);

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  /**
   * A singleton wrapper so that yarn cluster only bring up once across all tests in the YarnTestSuite.
   */
  @ClassRule
  public static final TwillTester TWILL_TESTER = new TwillTester(Configs.Keys.LOCATION_CACHE_DIR, ".cache") {
    private final AtomicInteger instances = new AtomicInteger();

    @Override
    protected void before() throws Throwable {
      if (instances.getAndIncrement() == 0) {
        super.before();
      }
    }

    @Override
    protected void after() {
      if (instances.decrementAndGet() == 0) {
        super.after();
      }
    }
  };

  @Rule
  public final TestName testName = new TestName();

  @Before
  public void beforeTest() {
    LOG.info("Before test {}", testName.getMethodName());
  }

  @After
  public void afterTest() {
    LOG.info("After test {}", testName.getMethodName());
  }

  @After
  public final void cleanupTest() {
    // Make sure all applications are stopped after a test case is executed, even it failed.
    TwillRunner twillRunner = TWILL_TESTER.getTwillRunner();
    for (TwillRunner.LiveInfo liveInfo : twillRunner.lookupLive()) {
      for (TwillController controller : liveInfo.getControllers()) {
        try {
          controller.terminate().get();
        } catch (Throwable t) {
          LOG.error("Failed to stop application {}", liveInfo.getApplicationName(), t);
        }
      }
    }
  }

  /**
   * Poll the given {@link Iterable} until its size is the same as the given count,
   * with a limited amount of polls. There is one second sleep between each poll.
   *
   * @param iterable the Iterable to poll
   * @param count the expected size
   * @param limit number of times to poll.
   * @param <T> type of the element inside the Iterable
   * @return true if the Iterable size is the same as the given count
   */
  public <T> boolean waitForSize(Iterable<T> iterable, int count, int limit) throws InterruptedException {
    int trial = 0;
    int size = Iterables.size(iterable);
    while (size != count && trial < limit) {
      LOG.info("Waiting for {} size {} == {}", iterable, size, count);
      TimeUnit.SECONDS.sleep(1);
      trial++;
      size = Iterables.size(iterable);
    }
    return trial < limit;
  }

  /**
   * Waits for a task returns the expected value.
   *
   * @param expected the expected value
   * @param callable the task to execute
   * @param timeout timeout of the wait
   * @param delay delay between calls to the task to poll for the latest value
   * @param unit unit for the timeout and delay
   * @param <T> type of the expected value
   * @throws Exception if the task through exception or timeout.
   */
  public <T> void waitFor(T expected, Callable<T> callable, long timeout, long delay, TimeUnit unit) throws Exception {
    Stopwatch stopwatch = new Stopwatch().start();
    while (callable.call() != expected && stopwatch.elapsedTime(unit) < timeout) {
      unit.sleep(delay);
    }
  }

  @SuppressWarnings("unchecked")
  public <T extends TwillRunner> T getTwillRunner() {
    return (T) TWILL_TESTER.getTwillRunner();
  }

  public List<NodeReport> getNodeReports() throws Exception {
    return TWILL_TESTER.getNodeReports();
  }

  public String getZKConnectionString() {
    return TWILL_TESTER.getZKConnectionString();
  }

  public ApplicationResourceUsageReport getApplicationResourceReport(String appId) throws Exception {
    return TWILL_TESTER.getApplicationResourceReport(appId);
  }
}
