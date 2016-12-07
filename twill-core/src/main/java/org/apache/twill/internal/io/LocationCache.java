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

import java.io.IOException;

/**
 * Defines caching of {@link Location}.
 */
public interface LocationCache {

  /**
   * Gets the {@link Location} represented by the given name. If there is no such name exists in the cache,
   * use the given {@link Loader} to populate the cache.
   *
   * @param name name of the caching entry
   * @param loader the {@link Loader} for populating the content for the cache entry
   * @return A {@link Location} associated with the cache name
   * @throws IOException If failed to load the {@link Location}.
   */
  Location get(String name, Loader loader) throws IOException;

  /**
   * A loader to load the content of the given name.
   */
  abstract class Loader {

    /**
     * Loads content into the given target {@link Location}.
     *
     * @param name name of the cache entry
     * @param targetLocation the target {@link Location} to store the content.
     * @throws IOException If failed to populate the content.
     */
    public abstract void load(String name, Location targetLocation) throws IOException;
  }
}
