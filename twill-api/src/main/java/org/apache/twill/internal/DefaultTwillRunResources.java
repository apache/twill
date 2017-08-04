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

import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.api.logging.LogEntry.Level;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *  Straightforward implementation of {@link org.apache.twill.api.TwillRunResources}.
 */
public class DefaultTwillRunResources implements TwillRunResources {
  private final String containerId;
  private final int instanceId;
  private final int virtualCores;
  private final int memoryMB;
  private final int maxHeapMemoryMB;
  private final String host;
  private final Integer debugPort;
  private final Map<String, LogEntry.Level> logLevels;

  /**
   * Constructor to create an instance of {@link DefaultTwillRunResources} with empty log levels.
   */
  public DefaultTwillRunResources(int instanceId, String containerId, int cores, int memoryMB, int maxHeapMemoryMB,
                                  String host, Integer debugPort) {
    this(instanceId, containerId, cores, memoryMB, maxHeapMemoryMB, host, debugPort,
         Collections.<String, Level>emptyMap());
  }

  public DefaultTwillRunResources(int instanceId, String containerId, int cores, int memoryMB, int maxHeapMemoryMB,
                                  String host, Integer debugPort, Map<String, LogEntry.Level> logLevels) {
    this.instanceId = instanceId;
    this.containerId = containerId;
    this.virtualCores = cores;
    this.memoryMB = memoryMB;
    this.maxHeapMemoryMB = maxHeapMemoryMB;
    this.host = host;
    this.debugPort = debugPort;
    this.logLevels = new HashMap<>(logLevels);
  }

  /**
   * @return instance id of the runnable.
   */
  @Override
  public int getInstanceId() {
    return instanceId;
  }

  /**
   * @return id of the container the runnable is running in.
   */
  @Override
  public String getContainerId() {
    return containerId;
  }

  /**
   * @return number of cores the runnable is allowed to use.  YARN must be at least v2.1.0 and
   *   it must be configured to use cgroups in order for this to be a reflection of truth.
   */
  @Override
  public int getVirtualCores() {
    return virtualCores;
  }

  /**
   * @return amount of memory in MB the runnable is allowed to use.
   */
  @Override
  public int getMemoryMB() {
    return memoryMB;
  }

  @Override
  public int getMaxHeapMemoryMB() {
    return maxHeapMemoryMB;
  }

  /**
   * @return the host the runnable is running on.
   */
  @Override
  public String getHost() {
    return host;
  }

  @Override
  public Integer getDebugPort() {
    return debugPort;
  }

  @Override
  @Deprecated
  @Nullable
  public Level getLogLevel() {
    return getLogLevels().get(Logger.ROOT_LOGGER_NAME);
  }

  @Override
  public Map<String, LogEntry.Level> getLogLevels() {
    return logLevels;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof TwillRunResources)) {
      return false;
    }
    TwillRunResources other = (TwillRunResources) o;
    return (instanceId == other.getInstanceId()) &&
      containerId.equals(other.getContainerId()) &&
      host.equals(other.getHost()) &&
      (virtualCores == other.getVirtualCores()) &&
      (memoryMB == other.getMemoryMB());
    // debugPort is ignored here
  }

  @Override
  public int hashCode() {
    int hash = 17;
    hash = 31 *  hash + containerId.hashCode();
    hash = 31 *  hash + host.hashCode();
    hash = 31 *  hash + (instanceId ^ (instanceId >>> 32));
    hash = 31 *  hash + (virtualCores ^ (virtualCores >>> 32));
    hash = 31 *  hash + (memoryMB ^ (memoryMB >>> 32));
    // debugPort is ignored here
    return hash;
  }

  @Override
  public String toString() {
    return "DefaultTwillRunResources{" +
      "containerId='" + containerId + '\'' +
      ", instanceId=" + instanceId +
      ", virtualCores=" + virtualCores +
      ", memoryMB=" + memoryMB +
      ", host='" + host + '\'' +
      ", debugPort=" + debugPort +
      ", logLevels=" + logLevels +
      '}';
  }
}
