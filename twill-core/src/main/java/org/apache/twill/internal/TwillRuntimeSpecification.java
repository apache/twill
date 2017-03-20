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
  private final int reservedMemory;
  private final String rmSchedulerAddr;
  private final Map<String, Map<String, String>> logLevels;
  private final Map<String, Integer> maxRetries;
  private final double minHeapRatio;
  private final boolean logCollectionEnabled;

  public TwillRuntimeSpecification(TwillSpecification twillSpecification, String fsUser, URI twillAppDir,
                                   String zkConnectStr, RunId twillRunId, String twillAppName,
                                   int reservedMemory, @Nullable String rmSchedulerAddr,
                                   Map<String, Map<String, String>> logLevels, Map<String, Integer> maxRetries,
                                   double minHeapRatio, boolean logCollectionEnabled) {
    this.twillSpecification = twillSpecification;
    this.fsUser = fsUser;
    this.twillAppDir = twillAppDir;
    this.zkConnectStr = zkConnectStr;
    this.twillRunId = twillRunId;
    this.twillAppName = twillAppName;
    this.reservedMemory = reservedMemory;
    this.rmSchedulerAddr = rmSchedulerAddr;
    this.logLevels = logLevels;
    this.maxRetries = maxRetries;
    this.minHeapRatio = minHeapRatio;
    this.logCollectionEnabled = logCollectionEnabled;
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

  public int getReservedMemory() {
    return reservedMemory;
  }

  public double getMinHeapRatio() {
    return minHeapRatio;
  }

  /**
   * Returns whether log collection is enabled.
   */
  public boolean isLogCollectionEnabled() {
    return logCollectionEnabled;
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
}
