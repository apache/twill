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
package org.apache.twill.internal;

import org.apache.twill.api.Configs;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

import java.net.URI;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Represents runtime specification of a {@link TwillApplication}.
 */
public class TwillRuntimeSpecification {

  private final TwillSpecification twillSpecification;

  private final String fsUser;
  private final URI twillAppDir;
  private final String zkConnectStr;
  private final RunId twillRunId;
  private final String twillAppName;
  private final String rmSchedulerAddr;
  private final Map<String, Map<String, String>> logLevels;
  private final Map<String, Integer> maxRetries;
  private final Map<String, String> config;
  private final Map<String, Map<String, String>> runnableConfigs;

  public TwillRuntimeSpecification(TwillSpecification twillSpecification, String fsUser, URI twillAppDir,
                                   String zkConnectStr, RunId twillRunId, String twillAppName,
                                   @Nullable String rmSchedulerAddr, Map<String, Map<String, String>> logLevels,
                                   Map<String, Integer> maxRetries, Map<String, String> config,
                                   Map<String, Map<String, String>> runnableConfigs) {
    this.twillSpecification = twillSpecification;
    this.fsUser = fsUser;
    this.twillAppDir = twillAppDir;
    this.zkConnectStr = zkConnectStr;
    this.twillRunId = twillRunId;
    this.twillAppName = twillAppName;
    this.rmSchedulerAddr = rmSchedulerAddr;
    this.logLevels = logLevels;
    this.maxRetries = maxRetries;
    this.config = config;
    this.runnableConfigs = runnableConfigs;
  }

  public TwillSpecification getTwillSpecification() {
    return twillSpecification;
  }

  public String getFsUser() {
    return fsUser;
  }

  public URI getTwillAppDir() {
    return twillAppDir;
  }

  public String getZkConnectStr() {
    return zkConnectStr;
  }

  public RunId getTwillAppRunId() {
    return twillRunId;
  }

  public String getTwillAppName() {
    return twillAppName;
  }

  /**
   * Returns the minimum heap ratio for the application master.
   */
  public double getAMMinHeapRatio() {
    return getMinHeapRatio(config, Configs.Defaults.HEAP_RESERVED_MIN_RATIO);
  }

  /**
   * Returns the minimum heap ratio for the given runnable.
   */
  public double getMinHeapRatio(String runnableName) {
    double ratio = getMinHeapRatio(runnableConfigs.get(runnableName), 0d);
    return ratio <= 0d ? getMinHeapRatio(config, Configs.Defaults.HEAP_RESERVED_MIN_RATIO) : ratio;
  }

  /**
   * Returns the reserved non-heap memory size in MB for the application master.
   */
  public int getAMReservedMemory() {
    return getReservedMemory(config, Configs.Keys.YARN_AM_RESERVED_MEMORY_MB,
                             Configs.Defaults.YARN_AM_RESERVED_MEMORY_MB);
  }

  /**
   * Returns the reserved non-heap memory size in MB for the given runnable.
   */
  public int getReservedMemory(String runnableName) {
    int memory = getReservedMemory(runnableConfigs.get(runnableName), Configs.Keys.JAVA_RESERVED_MEMORY_MB, -1);
    return memory < 0 ? getReservedMemory(config, Configs.Keys.JAVA_RESERVED_MEMORY_MB,
                                          Configs.Defaults.JAVA_RESERVED_MEMORY_MB) : memory;
  }

  /**
   * Returns whether log collection is enabled.
   */
  public boolean isLogCollectionEnabled() {
    return config.containsKey(Configs.Keys.LOG_COLLECTION_ENABLED) ?
      Boolean.parseBoolean(config.get(Configs.Keys.LOG_COLLECTION_ENABLED)) :
      Configs.Defaults.LOG_COLLECTION_ENABLED;
  }

  @Nullable
  public String getRmSchedulerAddr() {
    return rmSchedulerAddr;
  }

  public Map<String, Map<String, String>> getLogLevels() {
    return logLevels;
  }

  public Map<String, Integer> getMaxRetries() {
    return maxRetries;
  }

  /**
   * Returns the configuration for the application.
   */
  public Map<String, String> getConfig() {
    return config;
  }

  /**
   * Returns the configurations for each runnable.
   */
  public Map<String, Map<String, String>> getRunnableConfigs() {
    return runnableConfigs;
  }

  /**
   * Returns the ZK connection string for the Kafka used for log collections,
   * or {@code null} if log collection is disabled.
   */
  @Nullable
  public String getKafkaZKConnect() {
    if (!isLogCollectionEnabled()) {
      return null;
    }
    // When addressing TWILL-147, a field can be introduced to carry this value.
    return String.format("%s/%s/%s/kafka", getZkConnectStr(), getTwillAppName(), getTwillAppRunId());
  }

  /**
   * Returns the minimum heap ratio ({@link Configs.Keys#HEAP_RESERVED_MIN_RATIO}) based on the given configuration.
   */
  private double getMinHeapRatio(@Nullable Map<String, String> config, double defaultValue) {
    if (config == null || !config.containsKey(Configs.Keys.HEAP_RESERVED_MIN_RATIO)) {
      return defaultValue;
    }

    try {
      double ratio = Double.parseDouble(config.get(Configs.Keys.HEAP_RESERVED_MIN_RATIO));
      if (ratio <= 0d) {
        throw new IllegalArgumentException("Minimum heap ratio configured with key '" +
                                             Configs.Keys.HEAP_RESERVED_MIN_RATIO +
                                             "' must be > 0. It is configured to " + ratio);
      }
      return ratio;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Failed to parse the minimum heap ratio from configuration with key '" +
                                           Configs.Keys.HEAP_RESERVED_MIN_RATIO + "'", e);
    }
  }

  /**
   * Returns the reserved memory size based on the given configuration.
   */
  private int getReservedMemory(@Nullable Map<String, String> config, String key, int defaultValue) {
    if (config == null || !config.containsKey(key)) {
      return defaultValue;
    }

    try {
      int memory = Integer.parseInt(config.get(key));
      if (memory < 0) {
        throw new IllegalArgumentException("Reserved memory size configured with key '" + key +
                                             "' must be >= 0. It is configured to " + memory);
      }
      return memory;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Failed to parse the reserved memory size from configuration with key '" +
                                           key + "'", e);
    }
  }
}
