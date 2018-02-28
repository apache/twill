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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.twill.internal.appmaster.RunnableProcessLauncher;
import org.apache.twill.internal.yarn.ports.AMRMClient;
import org.apache.twill.internal.yarn.ports.AMRMClientImpl;
import org.apache.twill.internal.yarn.ports.AllocationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import javax.annotation.Nullable;

/**
 *
 */
public final class Hadoop20YarnAMClient extends AbstractYarnAMClient<AMRMClient.ContainerRequest> {

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

  private final AMRMClient amrmClient;
  private final YarnNMClient nmClient;
  private Resource maxCapability;
  private Resource minCapability;

  public Hadoop20YarnAMClient(Configuration conf) {
    super(ApplicationConstants.AM_CONTAINER_ID_ENV);

    this.amrmClient = new AMRMClientImpl(containerId.getApplicationAttemptId());
    this.amrmClient.init(conf);
    this.nmClient = new Hadoop20YarnNMClient(YarnRPC.create(conf), conf);
  }

  @Override
  protected ContainerId containerIdLookup(String containerIdStr) {
    return (ConverterUtils.toContainerId(containerIdStr));
  }

  @Override
  protected void startUp() throws Exception {
    Preconditions.checkNotNull(trackerAddr, "Tracker address not set.");
    Preconditions.checkNotNull(trackerUrl, "Tracker URL not set.");

    amrmClient.start();

    String url = String.format("%s:%d",
                               trackerUrl.getHost(),
                               trackerUrl.getPort() == -1 ? trackerUrl.getDefaultPort() : trackerUrl.getPort());
    RegisterApplicationMasterResponse response = amrmClient.registerApplicationMaster(trackerAddr.getHostName(),
                                                                                      trackerAddr.getPort(),
                                                                                      url);
    maxCapability = response.getMaximumResourceCapability();
    minCapability = response.getMinimumResourceCapability();
  }

  @Override
  protected void shutDown() throws Exception {
    amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, trackerUrl.toString());
    amrmClient.stop();
  }

  @Override
  public String getHost() {
    return System.getenv().get(ApplicationConstants.NM_HOST_ENV);
  }

  @Override
  public int getNMPort() {
    return Integer.parseInt(System.getenv().get(ApplicationConstants.NM_PORT_ENV));
  }

  @Override
  protected Resource adjustCapability(Resource resource) {
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
      LOG.info("Adjust memory requirement from {} to {} MB.", resource.getMemory(), updatedMemory);
      resource.setMemory(updatedMemory);
    }

    return resource;
  }

  @Override
  protected AMRMClient.ContainerRequest createContainerRequest(Priority priority, Resource capability,
                                                               @Nullable String[] hosts, @Nullable String[] racks,
                                                               boolean relaxLocality) {
    // Ignore relaxLocality param since the corresponding support is not present in Hadoop 2.0.
    return new AMRMClient.ContainerRequest(capability, hosts, racks, priority, 1);
  }

  @Override
  protected void addContainerRequest(AMRMClient.ContainerRequest request) {
    amrmClient.addContainerRequest(request);
  }

  @Override
  protected void removeContainerRequest(AMRMClient.ContainerRequest request) {
    amrmClient.removeContainerRequest(request);
  }

  @Override
  protected void updateBlacklist(List<String> blacklistAdditions, List<String> blacklistRemovals) {
    // An empty implementation since Blacklist is not supported in Hadoop-2.0.
    if (recordUnsupportedFeature("blacklist")) {
      LOG.warn("Blacklist is not supported in Hadoop 2.0");
    }
  }

  @Override
  protected AllocateResult doAllocate(float progress) throws Exception {
    AllocationResponse response = amrmClient.allocate(progress);
    List<RunnableProcessLauncher> launchers
      = Lists.newArrayListWithCapacity(response.getAllocatedContainers().size());

    for (Container container : response.getAllocatedContainers()) {
      launchers.add(new RunnableProcessLauncher(new Hadoop20YarnContainerInfo(container), nmClient));
    }

    List<YarnContainerStatus> completed = ImmutableList.copyOf(
      Iterables.transform(response.getCompletedContainersStatuses(), STATUS_TRANSFORM));

    return new AllocateResult(launchers, completed);
  }

  @Override
  protected void releaseAssignedContainer(YarnContainerInfo containerInfo) {
    Container container = containerInfo.getContainer();
    amrmClient.releaseAssignedContainer(container.getId());
  }
}
