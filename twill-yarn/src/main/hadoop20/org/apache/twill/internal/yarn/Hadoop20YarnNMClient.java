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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ContainerManager;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusRequest;
import org.apache.hadoop.yarn.api.protocolrecords.GetContainerStatusResponse;
import org.apache.hadoop.yarn.api.protocolrecords.StartContainerRequest;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainerRequest;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.exceptions.YarnRemoteException;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.Records;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 *
 */
public final class Hadoop20YarnNMClient implements YarnNMClient {

  private static final Logger LOG = LoggerFactory.getLogger(Hadoop20YarnNMClient.class);

  private final YarnRPC yarnRPC;
  private final Configuration yarnConf;

  public Hadoop20YarnNMClient(YarnRPC yarnRPC, Configuration yarnConf) {
    this.yarnRPC = yarnRPC;
    this.yarnConf = yarnConf;
  }

  @Override
  public Cancellable start(YarnContainerInfo containerInfo, YarnLaunchContext launchContext) {
    ContainerLaunchContext context = launchContext.getLaunchContext();
    context.setUser(System.getProperty("user.name"));

    Container container = containerInfo.getContainer();

    context.setContainerId(container.getId());
    context.setResource(container.getResource());

    StartContainerRequest startRequest = Records.newRecord(StartContainerRequest.class);
    startRequest.setContainerLaunchContext(context);

    ContainerManager manager = connectContainerManager(container);
    try {
      manager.startContainer(startRequest);
      return new ContainerTerminator(container, manager);
    } catch (YarnRemoteException e) {
      LOG.error("Error in launching process", e);
      throw Throwables.propagate(e);
    }

  }

  /**
   * Helper to connect to container manager (node manager).
   */
  private ContainerManager connectContainerManager(Container container) {
    String cmIpPortStr = String.format("%s:%d", container.getNodeId().getHost(), container.getNodeId().getPort());
    InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);
    return ((ContainerManager) yarnRPC.getProxy(ContainerManager.class, cmAddress, yarnConf));
  }

  private static final class ContainerTerminator implements Cancellable {

    private final Container container;
    private final ContainerManager manager;

    private ContainerTerminator(Container container, ContainerManager manager) {
      this.container = container;
      this.manager = manager;
    }

    @Override
    public void cancel() {
      LOG.info("Request to stop container {}.", container.getId());
      StopContainerRequest stopRequest = Records.newRecord(StopContainerRequest.class);
      stopRequest.setContainerId(container.getId());
      try {
        manager.stopContainer(stopRequest);
        boolean completed = false;
        while (!completed) {
          GetContainerStatusRequest statusRequest = Records.newRecord(GetContainerStatusRequest.class);
          statusRequest.setContainerId(container.getId());
          GetContainerStatusResponse statusResponse = manager.getContainerStatus(statusRequest);
          LOG.info("Container status: {} {}", statusResponse.getStatus(), statusResponse.getStatus().getDiagnostics());

          completed = (statusResponse.getStatus().getState() == ContainerState.COMPLETE);
        }
        LOG.info("Container {} stopped.", container.getId());
      } catch (YarnRemoteException e) {
        LOG.error("Fail to stop container {}", container.getId(), e);
        throw Throwables.propagate(e);
      }
    }
  }
}
