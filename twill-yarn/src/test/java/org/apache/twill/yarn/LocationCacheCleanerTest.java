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

import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.io.BasicLocationCache;
import org.apache.twill.internal.io.LocationCache;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;

/**
 * Unit test for {@link LocationCacheCleaner}.
 */
public class LocationCacheCleanerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testCleanup() throws IOException {
    LocationFactory lf = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    Location cacheBase = lf.create("cache");
    LocationCache.Loader cacheLoader = new LocationCache.Loader() {
      @Override
      public void load(String name, Location targetLocation) throws IOException {
        try (PrintStream printer = new PrintStream(targetLocation.getOutputStream(), true, "UTF-8")) {
          printer.println(name);
        }
      }
    };

    // Create a cache with a session
    LocationCache cache = new BasicLocationCache(cacheBase.append("old"));
    cache.get("old", cacheLoader);

    // Create a new cache with a different session
    String sessionId = "new";
    cache = new BasicLocationCache(cacheBase.append(sessionId));
    cache.get("new", cacheLoader);

    // Cleanup all files
    LocationCacheCleaner cleaner = new LocationCacheCleaner(new Configuration(), cacheBase,
                                                            sessionId, location -> true);
    // The first cleanup will add files to pending deletion
    cleaner.cleanup(Long.MAX_VALUE);
    // Second call to cleanup will delete files and old sessions directories.
    cleaner.cleanup(Long.MAX_VALUE);

    // The cache directory should only have the new session directory
    Assert.assertEquals(Collections.singletonList(cacheBase.append(sessionId)), cacheBase.list());
    // The new session cache directory should be empty
    Assert.assertTrue(cacheBase.append(sessionId).list().isEmpty());
  }
}
