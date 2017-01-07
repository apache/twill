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
package org.apache.twill.internal.appmaster;

import org.apache.twill.api.LocalFile;

import java.util.List;

/**
 * Represents data being stored in the live node of the application master.
 */
public final class ApplicationMasterLiveNodeData {

  private final int appId;
  private final long appIdClusterTime;
  private final String containerId;
  private final List<LocalFile> localFiles;

  public ApplicationMasterLiveNodeData(int appId, long appIdClusterTime,
                                       String containerId, List<LocalFile> localFiles) {
    this.appId = appId;
    this.appIdClusterTime = appIdClusterTime;
    this.containerId = containerId;
    this.localFiles = localFiles;
  }

  public int getAppId() {
    return appId;
  }

  public long getAppIdClusterTime() {
    return appIdClusterTime;
  }

  public String getContainerId() {
    return containerId;
  }

  public List<LocalFile> getLocalFiles() {
    return localFiles;
  }

  @Override
  public String toString() {
    return "ApplicationMasterLiveNodeData{" +
      "appId=" + appId +
      ", appIdClusterTime=" + appIdClusterTime +
      ", containerId='" + containerId + '\'' +
      ", localFiles=" + localFiles +
      '}';
  }
}
