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
package org.apache.twill.internal.yarn;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper class for {@link NMClient} for Hadoop version 2.1 or greater.
 */
public final class Hadoop21YarnNMClient extends AbstractIdleService implements YarnNMClient {

  private static final Logger LOG = LoggerFactory.getLogger(Hadoop21YarnNMClient.class);

  private final NMClient nmClient;

  public Hadoop21YarnNMClient(Configuration configuration) {
    this.nmClient = NMClient.createNMClient();
    nmClient.init(configuration);
  }

  @Override
  public Cancellable start(YarnContainerInfo containerInfo, YarnLaunchContext launchContext) {
    try {
      Container container = containerInfo.getContainer();
      nmClient.startContainer(container, launchContext.<ContainerLaunchContext>getLaunchContext());
      return new ContainerTerminator(container, nmClient);
    } catch (Exception e) {
      LOG.error("Error in launching process", e);
      throw Throwables.propagate(e);
    }

  }

  @Override
  protected void startUp() throws Exception {
    nmClient.start();
  }

  @Override
  protected void shutDown() throws Exception {
    nmClient.stop();
  }

  private static final class ContainerTerminator implements Cancellable {

    private final Container container;
    private final NMClient nmClient;

    private ContainerTerminator(Container container, NMClient nmClient) {
      this.container = container;
      this.nmClient = nmClient;
    }

    @Override
    public void cancel() {
      LOG.info("Request to stop container {}.", container.getId());

      try {
        nmClient.stopContainer(container.getId(), container.getNodeId());
        boolean completed = false;
        while (!completed) {
          ContainerStatus status = nmClient.getContainerStatus(container.getId(), container.getNodeId());
          LOG.info("Container status: {} {}", status, status.getDiagnostics());

          completed = (status.getState() == ContainerState.COMPLETE);
        }
        LOG.info("Container {} stopped.", container.getId());
      } catch (Exception e) {
        LOG.error("Fail to stop container {}", container.getId(), e);
        throw Throwables.propagate(e);
      }
    }
  }
}
