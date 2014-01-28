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
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.yarn.AbstractYarnProcessLauncher;
import org.apache.twill.internal.yarn.YarnLaunchContext;
import org.apache.twill.internal.yarn.YarnUtils;

import java.util.Map;

/**
 * A {@link org.apache.twill.internal.ProcessLauncher} for launching Application Master from the client.
 */
public final class ApplicationMasterProcessLauncher extends AbstractYarnProcessLauncher<ApplicationId> {

  private final ApplicationSubmitter submitter;

  public ApplicationMasterProcessLauncher(ApplicationId appId, ApplicationSubmitter submitter) {
    super(appId);
    this.submitter = submitter;
  }

  @Override
  protected boolean useArchiveSuffix() {
    return false;
  }

  @Override
  @SuppressWarnings("unchecked")
  protected <R> ProcessController<R> doLaunch(YarnLaunchContext launchContext) {
    final ApplicationId appId = getContainerInfo();

    // Set the resource requirement for AM
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(Constants.APP_MASTER_MEMORY_MB);
    YarnUtils.setVirtualCores(capability, 1);

    // Put in extra environments
    Map<String, String> env = ImmutableMap.<String, String>builder()
      .putAll(launchContext.getEnvironment())
      .put(EnvKeys.YARN_APP_ID, Integer.toString(appId.getId()))
      .put(EnvKeys.YARN_APP_ID_CLUSTER_TIME, Long.toString(appId.getClusterTimestamp()))
      .put(EnvKeys.YARN_APP_ID_STR, appId.toString())
      .put(EnvKeys.YARN_CONTAINER_MEMORY_MB, Integer.toString(Constants.APP_MASTER_MEMORY_MB))
      .put(EnvKeys.YARN_CONTAINER_VIRTUAL_CORES, Integer.toString(YarnUtils.getVirtualCores(capability)))
      .build();

    launchContext.setEnvironment(env);
    return (ProcessController<R>) submitter.submit(launchContext, capability);
  }
}
