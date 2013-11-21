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

import org.apache.twill.internal.ProcessLauncher;
import org.apache.twill.internal.appmaster.RunnableProcessLauncher;
import org.apache.twill.internal.yarn.ports.AMRMClient;
import org.apache.twill.internal.yarn.ports.AMRMClientImpl;
import org.apache.twill.internal.yarn.ports.AllocationResponse;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.List;
import java.util.UUID;

/**
 *
 */
public final class Hadoop20YarnAMClient extends AbstractIdleService implements YarnAMClient {

  private static final Logger LOG = LoggerFactory.getLogger(Hadoop20YarnAMClient.class);
  private static final Function<ContainerStatus, YarnContainerStatus> STATUS_TRANSFORM;

  static {
    STATUS_TRANSFORM = new Function<ContainerStatus, YarnContainerStatus>() {
      @Override
      public YarnContainerStatus apply(ContainerStatus status) {
        return new Hadoop20YarnContainerStatus(status);
      }
    };
  }

  private final ContainerId containerId;
  private final Multimap<String, AMRMClient.ContainerRequest> containerRequests;
  private final AMRMClient amrmClient;
  private final YarnNMClient nmClient;
  private InetSocketAddress trackerAddr;
  private URL trackerUrl;
  private Resource maxCapability;
  private Resource minCapability;

  public Hadoop20YarnAMClient(Configuration conf) {
    String masterContainerId = System.getenv().get(ApplicationConstants.AM_CONTAINER_ID_ENV);
    Preconditions.checkArgument(masterContainerId != null,
                                "Missing %s from environment", ApplicationConstants.AM_CONTAINER_ID_ENV);
    this.containerId = ConverterUtils.toContainerId(masterContainerId);
    this.containerRequests = ArrayListMultimap.create();

    this.amrmClient = new AMRMClientImpl(containerId.getApplicationAttemptId());
    this.amrmClient.init(conf);
    this.nmClient = new Hadoop20YarnNMClient(YarnRPC.create(conf), conf);
  }

  @Override
  protected void startUp() throws Exception {
    Preconditions.checkNotNull(trackerAddr, "Tracker address not set.");
    Preconditions.checkNotNull(trackerUrl, "Tracker URL not set.");

    amrmClient.start();

    RegisterApplicationMasterResponse response = amrmClient.registerApplicationMaster(trackerAddr.getHostName(),
                                                                                      trackerAddr.getPort(),
                                                                                      trackerUrl.toString());
    maxCapability = response.getMaximumResourceCapability();
    minCapability = response.getMinimumResourceCapability();
  }

  @Override
  protected void shutDown() throws Exception {
    amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, trackerUrl.toString());
    amrmClient.stop();
  }

  @Override
  public ContainerId getContainerId() {
    return containerId;
  }

  @Override
  public String getHost() {
    return System.getenv().get(ApplicationConstants.NM_HOST_ENV);
  }

  @Override
  public void setTracker(InetSocketAddress trackerAddr, URL trackerUrl) {
    this.trackerAddr = trackerAddr;
    this.trackerUrl = trackerUrl;
  }

  @Override
  public synchronized void allocate(float progress, AllocateHandler handler) throws Exception {
    AllocationResponse response = amrmClient.allocate(progress);
    List<ProcessLauncher<YarnContainerInfo>> launchers
      = Lists.newArrayListWithCapacity(response.getAllocatedContainers().size());

    for (Container container : response.getAllocatedContainers()) {
      launchers.add(new RunnableProcessLauncher(new Hadoop20YarnContainerInfo(container), nmClient));
    }

    if (!launchers.isEmpty()) {
      handler.acquired(launchers);

      // If no process has been launched through the given launcher, return the container.
      for (ProcessLauncher<YarnContainerInfo> l : launchers) {
        // This cast always works.
        RunnableProcessLauncher launcher = (RunnableProcessLauncher) l;
        if (!launcher.isLaunched()) {
          Container container = launcher.getContainerInfo().getContainer();
          LOG.info("Nothing to run in container, releasing it: {}", container);
          amrmClient.releaseAssignedContainer(container.getId());
        }
      }
    }

    List<YarnContainerStatus> completed = ImmutableList.copyOf(
      Iterables.transform(response.getCompletedContainersStatuses(), STATUS_TRANSFORM));
    if (!completed.isEmpty()) {
      handler.completed(completed);
    }
  }

  @Override
  public ContainerRequestBuilder addContainerRequest(Resource capability) {
    return addContainerRequest(capability, 1);
  }

  @Override
  public ContainerRequestBuilder addContainerRequest(Resource capability, int count) {
    return new ContainerRequestBuilder(adjustCapability(capability), count) {
      @Override
      public String apply() {
        synchronized (Hadoop20YarnAMClient.this) {
          String id = UUID.randomUUID().toString();

          String[] hosts = this.hosts.isEmpty() ? null : this.hosts.toArray(new String[this.hosts.size()]);
          String[] racks = this.racks.isEmpty() ? null : this.racks.toArray(new String[this.racks.size()]);

          for (int i = 0; i < count; i++) {
            AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, hosts, racks,
                                                                                  priority, 1);
            containerRequests.put(id, request);
            amrmClient.addContainerRequest(request);
          }

          return id;
        }
      }
    };
  }

  @Override
  public synchronized void completeContainerRequest(String id) {
    for (AMRMClient.ContainerRequest request : containerRequests.removeAll(id)) {
      amrmClient.removeContainerRequest(request);
    }
  }

  private Resource adjustCapability(Resource resource) {
    int cores = YarnUtils.getVirtualCores(resource);
    int updatedCores = Math.max(Math.min(cores, YarnUtils.getVirtualCores(maxCapability)),
                                YarnUtils.getVirtualCores(minCapability));
    // Try and set the virtual cores, which older versions of YARN don't support this.
    if (cores != updatedCores && YarnUtils.setVirtualCores(resource, updatedCores)) {
      LOG.info("Adjust virtual cores requirement from {} to {}.", cores, updatedCores);
    }

    int updatedMemory = Math.min(resource.getMemory(), maxCapability.getMemory());
    int minMemory = minCapability.getMemory();
    updatedMemory = (int) Math.ceil(((double) updatedMemory / minMemory)) * minMemory;

    if (resource.getMemory() != updatedMemory) {
      resource.setMemory(updatedMemory);
      LOG.info("Adjust memory requirement from {} to {} MB.", resource.getMemory(), updatedMemory);
    }

    return resource;
  }
}
