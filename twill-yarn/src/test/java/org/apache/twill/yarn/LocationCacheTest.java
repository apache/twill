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

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.api.AbstractTwillRunnable;
import org.apache.twill.api.Configs;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.io.LocationCache;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link LocationCache} usage in {@link YarnTwillRunnerService}.
 */
public class LocationCacheTest {

  // Create a TwillTester with LocationCache enabled
  @ClassRule
  public static final TwillTester TWILL_TESTER = new TwillTester(Configs.Keys.LOCATION_CACHE_DIR, ".cache");

  @Test(timeout = 120000L)
  public void testLocationCache() throws Exception {
    TwillRunner twillRunner = TWILL_TESTER.getTwillRunner();

    // Start the runnable
    TwillController controller = twillRunner.prepare(new BlockingTwillRunnable())
      .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
      .start();

    // Wait until the runnable is runnable
    String runnableName = BlockingTwillRunnable.class.getSimpleName();
    ResourceReport resourceReport = controller.getResourceReport();
    while (resourceReport == null || resourceReport.getRunnableResources(runnableName).isEmpty()) {
      TimeUnit.SECONDS.sleep(1);
      resourceReport = controller.getResourceReport();
    }

    long startTime = System.currentTimeMillis();

    // Inspect the cache directory, there should be a directory, which is the current session
    // inside that directory, there should be three files, launcher.jar, twill.jar and an application jar
    LocationFactory locationFactory = TWILL_TESTER.createLocationFactory();
    Location cacheBase = locationFactory.create(".cache");

    List<Location> cacheDirs = cacheBase.list();
    Assert.assertEquals(1, cacheDirs.size());

    Location currentSessionCache = cacheDirs.get(0);
    Assert.assertEquals(3, currentSessionCache.list().size());

    // Force a cleanup of cache. The first call is to collect the locations to be cleanup.
    // The second call is the actual cleanup.
    ((YarnTwillRunnerService) twillRunner).forceLocationCacheCleanup(startTime);
    ((YarnTwillRunnerService) twillRunner).forceLocationCacheCleanup(startTime +
                                                                       Configs.Defaults.LOCATION_CACHE_EXPIRY_MS);

    // Since the app is still runnable, no files in the cache should get removed.
    Assert.assertEquals(3, currentSessionCache.list().size());

    // Stop the app
    controller.terminate().get();

    // Force a cleanup of cache. The first call is to collect the locations to be cleanup.
    // The second call is the actual cleanup.
    ((YarnTwillRunnerService) twillRunner).forceLocationCacheCleanup(startTime);
    ((YarnTwillRunnerService) twillRunner).forceLocationCacheCleanup(startTime +
                                                                       Configs.Defaults.LOCATION_CACHE_EXPIRY_MS);

    // Since the app is stopped, there should only be two files, the launcher.jar and twill.jar, as they
    // will never get removed for the current session.
    Set<Location> cachedLocations = new HashSet<>(currentSessionCache.list());
    Assert.assertEquals(2, cachedLocations.size());
    Assert.assertTrue(cachedLocations.contains(currentSessionCache.append(Constants.Files.LAUNCHER_JAR)));
    Assert.assertTrue(cachedLocations.contains(currentSessionCache.append(Constants.Files.TWILL_JAR)));

    // Start another YarnTwillRunnerService
    TwillRunnerService newTwillRunner = TWILL_TESTER.createTwillRunnerService();
    newTwillRunner.start();

    // Force a cleanup using the antique expiry. The list of locations that need to be cleanup was already
    // collected when the new twill runner was started
    ((YarnTwillRunnerService) newTwillRunner)
      .forceLocationCacheCleanup(System.currentTimeMillis() + Configs.Defaults.LOCATION_CACHE_ANTIQUE_EXPIRY_MS);

    // Now there shouldn't be any file under the current session cache directory
    Assert.assertTrue(currentSessionCache.list().isEmpty());
  }

  /**
   * A runnable that blocks until stopped explicitly.
   */
  public static final class BlockingTwillRunnable extends AbstractTwillRunnable {

    private final CountDownLatch stopLatch = new CountDownLatch(1);

    @Override
    public void run() {
      Uninterruptibles.awaitUninterruptibly(stopLatch);
    }

    @Override
    public void stop() {
      stopLatch.countDown();
    }
  }
}
