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
import org.apache.twill.internal.utils.Paths;

import java.io.IOException;

/**
 * A implementation of {@link LocationCache} that never cache any content.
 * It always invokes {@link Loader#load(String, Location)} to load the content.
 */
public class NoCachingLocationCache implements LocationCache {

  private final Location baseDir;

  public NoCachingLocationCache(Location baseDir) {
    this.baseDir = baseDir;
  }

  @Override
  public Location get(String name, Loader loader) throws IOException {
    String suffix = Paths.getExtension(name);
    String prefix = name.substring(0, name.length() - suffix.length() - 1);
    Location targetLocation = baseDir.append(prefix).getTempFile('.' + suffix);
    loader.load(name, targetLocation);
    return targetLocation;
  }
}
