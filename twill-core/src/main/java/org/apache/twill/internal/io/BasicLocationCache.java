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
package org.apache.twill.internal.io;

import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A straightforward implementation of {@link LocationCache} that simply use location existence as the cache
 * indicator.
 */
public class BasicLocationCache implements LocationCache {

  private static final Logger LOG = LoggerFactory.getLogger(BasicLocationCache.class);

  private final Location cacheDir;

  public BasicLocationCache(Location cacheDir) {
    this.cacheDir = cacheDir;
  }

  @Override
  public synchronized Location get(String name, Loader loader) throws IOException {
    Location location = cacheDir.append(name);
    if (location.exists()) {
      LOG.debug("Cache hit for {} in {}", name, location);
      return location;
    }

    LOG.debug("Cache miss for {}. Use Loader to save to {}", name, location);
    loader.load(name, location);
    return location;
  }
}
