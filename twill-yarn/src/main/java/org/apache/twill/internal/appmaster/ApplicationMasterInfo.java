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
package org.apache.twill.internal.appmaster;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.twill.internal.ResourceCapability;

/**
 * Represents information of the application master.
 */
public class ApplicationMasterInfo implements ResourceCapability {

  private final ApplicationId appId;
  private final int memoryMB;
  private final int virtualCores;

  public ApplicationMasterInfo(ApplicationId appId, int memoryMB, int virtualCores) {
    this.appId = appId;
    this.memoryMB = memoryMB;
    this.virtualCores = virtualCores;
  }

  /**
   * Returns the application ID for the YARN application.
   */
  public ApplicationId getAppId() {
    return appId;
  }

  @Override
  public int getMemoryMB() {
    return memoryMB;
  }

  @Override
  public int getVirtualCores() {
    return virtualCores;
  }

  @Override
  public String toString() {
    return "ApplicationMasterInfo{" +
      "appId=" + appId +
      ", memoryMB=" + memoryMB +
      ", virtualCores=" + virtualCores +
      '}';
  }
}
