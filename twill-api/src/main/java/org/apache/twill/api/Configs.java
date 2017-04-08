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
package org.apache.twill.api;

import java.util.concurrent.TimeUnit;

/**
 * Defines keys and default values constants being used for configuration.
 */
public final class Configs {

  /**
   * Defines keys being used in configuration.
   */
  public static final class Keys {
    /**
     * Size in MB of reserved memory for Java process (non-heap memory).
     */
    public static final String JAVA_RESERVED_MEMORY_MB = "twill.java.reserved.memory.mb";

    /**
     * Configurable ratio between Heap and Reserved Memory.
     */
    public static final String HEAP_RESERVED_MIN_RATIO = "twill.java.heap.memory.ratio";

    /**
     * Set this to false to disable the secure store updates done by default.
     */
    public static final String SECURE_STORE_UPDATE_LOCATION_ENABLED = "twill.secure.store.update.location.enabled";

    /**
     * Specifies the local directory for twill to store files generated at runtime.
     */
    public static final String LOCAL_STAGING_DIRECTORY = "twill.local.staging.dir";

    /**
     * Setting caching directory name for location cache.
     */
    public static final String LOCATION_CACHE_DIR = "twill.location.cache.dir";

    /**
     * Setting the expiration time in milliseconds of unused files in the location cache.
     * The value should be as long as the period when the same application will get launched again.
     */
    public static final String LOCATION_CACHE_EXPIRY_MS = "twill.location.cache.expiry.ms";

    /**
     * Setting the expiration time in milliseconds of unused files created by older runs in the location cache.
     * The value should be relatively short as those cache files won't get reused after those applications
     * that are using files completed. This expiry is mainly to workaround the delay that twill detects
     * the set of all running applications from ZK.
     */
    public static final String LOCATION_CACHE_ANTIQUE_EXPIRY_MS = "twill.location.cache.antique.expiry.ms";

    /**
     * Size in MB for the memory size of the YARN application master container.
     */
    public static final String YARN_AM_MEMORY_MB = "twill.yarn.am.memory.mb";

    /**
     * Size in MB for the reserved non-heap memory size for the YARN application Java process.
     * The actual reserved memory size is limited by the {@link #HEAP_RESERVED_MIN_RATIO}.
     */
    public static final String YARN_AM_RESERVED_MEMORY_MB = "twill.yarn.am.reserved.memory.mb";

    /**
     * Setting for enabling log collection.
     */
    public static final String LOG_COLLECTION_ENABLED = "twill.log.collection.enabled";

    /**
     * The maximum number of FileContext object cached by the FileContextLocationFactory.
     */
    public static final String FILE_CONTEXT_CACHE_MAX_SIZE = "twill.file.context.cache.max.size";

    private Keys() {
    }
  }

  /**
   * Defines default configuration values.
   */
  public static final class Defaults {
    /**
     * Default have 200MB reserved for Java process.
     */
    public static final int JAVA_RESERVED_MEMORY_MB = 200;

    public static final double HEAP_RESERVED_MIN_RATIO = 0.7d;
    /**
     * Default use the system temp directory for local staging files.
     */
    public static final String LOCAL_STAGING_DIRECTORY = System.getProperty("java.io.tmpdir");

    /**
     * Default expiration is one day for location cache.
     */
    public static final long LOCATION_CACHE_EXPIRY_MS = TimeUnit.DAYS.toMillis(1);

    /**
     * Default expiration is five minutes for location cache created by different twill runner.
     */
    public static final long LOCATION_CACHE_ANTIQUE_EXPIRY_MS = TimeUnit.MINUTES.toMillis(5);

    /**
     * Default AM container memory size.
     */
    public static final int YARN_AM_MEMORY_MB = 512;

    /**
     * Default AM JVM reserved memory.
     */
    public static final int YARN_AM_RESERVED_MEMORY_MB = 150;

    /**
     * Default to enable log collection.
     */
    public static final boolean LOG_COLLECTION_ENABLED = true;

    /**
     * Default size of the file context cache.
     */
    public static final int FILE_CONTEXT_CACHE_MAX_SIZE = 100;


    private Defaults() {
    }
  }

  private Configs() {
  }
}
