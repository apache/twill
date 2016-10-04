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

import com.google.common.base.Objects;
import com.google.common.collect.Maps;
import org.apache.twill.common.Cancellable;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.ProcessController;
import org.apache.twill.internal.yarn.AbstractYarnProcessLauncher;
import org.apache.twill.internal.yarn.YarnContainerInfo;
import org.apache.twill.internal.yarn.YarnLaunchContext;
import org.apache.twill.internal.yarn.YarnNMClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 */
public final class RunnableProcessLauncher extends AbstractYarnProcessLauncher<YarnContainerInfo> {

  private static final Logger LOG = LoggerFactory.getLogger(RunnableProcessLauncher.class);

  private final YarnContainerInfo containerInfo;
  private final YarnNMClient nmClient;
  private boolean launched;

  public RunnableProcessLauncher(YarnContainerInfo containerInfo, YarnNMClient nmClient) {
    super(containerInfo);
    this.containerInfo = containerInfo;
    this.nmClient = nmClient;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("container", containerInfo)
      .toString();
  }

  @Override
  protected <R> ProcessController<R> doLaunch(YarnLaunchContext launchContext) {
    Map<String, String> env = Maps.newHashMap(launchContext.getEnvironment());

    // Set extra environments
    env.put(EnvKeys.YARN_CONTAINER_ID, containerInfo.getId());
    env.put(EnvKeys.YARN_CONTAINER_HOST, containerInfo.getHost().getHostName());
    env.put(EnvKeys.YARN_CONTAINER_PORT, Integer.toString(containerInfo.getPort()));
    env.put(EnvKeys.YARN_CONTAINER_MEMORY_MB, Integer.toString(containerInfo.getMemoryMB()));
    env.put(EnvKeys.YARN_CONTAINER_VIRTUAL_CORES, Integer.toString(containerInfo.getVirtualCores()));

    launchContext.setEnvironment(env);

    LOG.info("Launching in container {} at {}:{}, {}",
             containerInfo.getId(), containerInfo.getHost().getHostName(),
             containerInfo.getPort(), launchContext.getCommands());
    final Cancellable cancellable = nmClient.start(containerInfo, launchContext);
    launched = true;

    return new ProcessController<R>() {
      @Override
      public void close() throws Exception {
        // no-op
      }

      @Override
      public R getReport() {
        // No reporting support for runnable launch yet.
        return null;

      }

      @Override
      public void cancel() {
        cancellable.cancel();
      }
    };
  }

  public boolean isLaunched() {
    return launched;
  }
}
