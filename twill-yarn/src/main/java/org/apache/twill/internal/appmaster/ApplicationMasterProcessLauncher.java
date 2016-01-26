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

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.yarn.AbstractYarnProcessLauncher;
import org.apache.twill.internal.yarn.YarnLaunchContext;

import java.util.Map;

/**
 * A {@link org.apache.twill.internal.ProcessLauncher} for launching Application Master from the client.
 */
public final class ApplicationMasterProcessLauncher extends AbstractYarnProcessLauncher<ApplicationMasterInfo> {

  private final ApplicationSubmitter submitter;

  public ApplicationMasterProcessLauncher(ApplicationMasterInfo info, ApplicationSubmitter submitter) {
    super(info);
    this.submitter = submitter;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <R> ProcessController<R> doLaunch(YarnLaunchContext launchContext) {
    ApplicationMasterInfo appMasterInfo = getContainerInfo();
    ApplicationId appId = appMasterInfo.getAppId();

    // Put in extra environments
    Map<String, String> env = ImmutableMap.<String, String>builder()
      .putAll(launchContext.getEnvironment())
      .put(EnvKeys.YARN_APP_ID, Integer.toString(appId.getId()))
      .put(EnvKeys.YARN_APP_ID_CLUSTER_TIME, Long.toString(appId.getClusterTimestamp()))
      .put(EnvKeys.YARN_APP_ID_STR, appId.toString())
      .put(EnvKeys.YARN_CONTAINER_MEMORY_MB, Integer.toString(appMasterInfo.getMemoryMB()))
      .put(EnvKeys.YARN_CONTAINER_VIRTUAL_CORES, Integer.toString(appMasterInfo.getVirtualCores()))
      .build();

    launchContext.setEnvironment(env);
    return (ProcessController<R>) submitter.submit(launchContext);
  }
}
