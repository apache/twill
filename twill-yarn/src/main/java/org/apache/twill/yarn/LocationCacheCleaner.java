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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.Configs;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.io.LocationCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Responsible for cleanup of {@link LocationCache}.
 */
final class LocationCacheCleaner extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(LocationCacheCleaner.class);

  private final Location cacheBaseLocation;
  private final String sessionId;
  private final long expiry;
  private final long antiqueExpiry;
  private final Predicate<Location> cleanupPredicate;
  private final Set<PendingCleanup> pendingCleanups;
  private ScheduledExecutorService scheduler;

  LocationCacheCleaner(Configuration config, Location cacheBaseLocation,
                       String sessionId, Predicate<Location> cleanupPredicate) {
    this.cacheBaseLocation = cacheBaseLocation;
    this.sessionId = sessionId;
    this.expiry = config.getLong(Configs.Keys.LOCATION_CACHE_EXPIRY_MS,
                                 Configs.Defaults.LOCATION_CACHE_EXPIRY_MS);
    this.antiqueExpiry = config.getLong(Configs.Keys.LOCATION_CACHE_ANTIQUE_EXPIRY_MS,
                                        Configs.Defaults.LOCATION_CACHE_ANTIQUE_EXPIRY_MS);
    this.cleanupPredicate = cleanupPredicate;
    this.pendingCleanups = new HashSet<>();
  }

  @Override
  protected void startUp() throws Exception {
    scheduler = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("location-cache-cleanup"));
    scheduler.execute(new Runnable() {
      @Override
      public void run() {
        long currentTime = System.currentTimeMillis();
        cleanup(currentTime);

        // By default, run the cleanup at half of the expiry
        long scheduleDelay = expiry / 2;
        for (PendingCleanup pendingCleanup : pendingCleanups) {
          // If there is any pending cleanup that needs to be cleanup early, schedule the run earlier.
          if (pendingCleanup.getExpireTime() - currentTime < scheduleDelay) {
            scheduleDelay = pendingCleanup.getExpireTime() - currentTime;
          }
        }
        scheduler.schedule(this, scheduleDelay, TimeUnit.MILLISECONDS);
      }
    });
  }

  @Override
  protected void shutDown() throws Exception {
    scheduler.shutdownNow();
  }

  @VisibleForTesting
  void forceCleanup(final long currentTime) {
    Futures.getUnchecked(scheduler.submit(new Runnable() {
      @Override
      public void run() {
        cleanup(currentTime);
      }
    }));
  }

  /**
   * Performs cleanup based on the given time.
   */
  private void cleanup(long currentTime) {
    // First go through the pending cleanup list and remove those that can be removed
    Iterator<PendingCleanup> iterator = pendingCleanups.iterator();
    while (iterator.hasNext()) {
      PendingCleanup pendingCleanup = iterator.next();

      // If rejected by the predicate, it means it is being used, hence remove it from the pending cleanup list.
      if (!cleanupPredicate.apply(pendingCleanup.getLocation())) {
        iterator.remove();
      } else {
        try {
          // If time is up for the pending entry, the location will be deleted,
          // hence can be removed from the pending cleanup list.
          // Otherwise retain it for the next cycle.
          if (pendingCleanup.deleteIfExpired(currentTime)) {
            iterator.remove();
          }
        } catch (IOException e) {
          // Log and retain the entry so that another attempt on deletion will be made in next cleanup cycle
          LOG.warn("Failed to delete {}", pendingCleanup.getLocation(), e);
        }
      }
    }

    // Then collects the next set of locations to be removed
    try {
      for (Location cacheDir : cacheBaseLocation.list()) {
        try {
          for (Location location : cacheDir.list()) {
            if (cleanupPredicate.apply(location)) {
              long expireTime = currentTime;
              if (cacheDir.getName().equals(sessionId)) {
                expireTime += expiry;
              } else {
                // If the cache entry is from different YarnTwillRunnerService session, use the anti expiry time.
                expireTime += antiqueExpiry;
              }
              // If the location is already pending for cleanup, this won't update the expire time as
              // the comparison of PendingCleanup is only by location.
              pendingCleanups.add(new PendingCleanup(location, expireTime));
            }
          }
        } catch (IOException e) {
          LOG.warn("Failed to list cache content from {}", cacheDir, e);
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to list cache directories from {}", cacheBaseLocation, e);
    }
  }

  /**
   * Class for holding information about cache location that is pending to be removed.
   * The equality and hash code is only based on the location.
   */
  private static final class PendingCleanup {
    private final Location location;
    private final long expireTime;

    PendingCleanup(Location location, long expireTime) {
      this.location = location;
      this.expireTime = expireTime;
    }

    Location getLocation() {
      return location;
    }

    long getExpireTime() {
      return expireTime;
    }

    /**
     * Deletes the location in this class if it is expired according to the given current time.
     *
     * @return true if expired and attempt was made to delete the location
     */
    boolean deleteIfExpired(long currentTime) throws IOException {
      if (currentTime < expireTime) {
        return false;
      }
      if (location.delete()) {
        LOG.debug("Cached location removed {}", location);
      } else {
        // It's ok to have delete returns false, e.g. if the location is removed by some other process
        LOG.debug("Failed to delete cached location {}", location);
      }
      return true;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PendingCleanup that = (PendingCleanup) o;
      return location.equals(that.location);
    }

    @Override
    public int hashCode() {
      return Objects.hash(location);
    }
  }
}
