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
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.twill.internal.appmaster.RunnableProcessLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Wrapper class for {@link AMRMClient} for Hadoop version 2.1 or greater.
 */
public class Hadoop21YarnAMClient extends AbstractYarnAMClient<AMRMClient.ContainerRequest> {

  private static final Logger LOG = LoggerFactory.getLogger(Hadoop21YarnAMClient.class);

  protected static final Function<ContainerStatus, YarnContainerStatus> STATUS_TRANSFORM;

  static {
    STATUS_TRANSFORM = new Function<ContainerStatus, YarnContainerStatus>() {
      @Override
      public YarnContainerStatus apply(ContainerStatus status) {
        return new Hadoop21YarnContainerStatus(status);
      }
    };
  }

  @Override
  protected ContainerId containerIdLookup(String containerIdStr) {
    return (ConverterUtils.toContainerId(containerIdStr));
  }

  protected final AMRMClient<AMRMClient.ContainerRequest> amrmClient;
  protected final Hadoop21YarnNMClient nmClient;
  protected Resource maxCapability;

  public Hadoop21YarnAMClient(Configuration conf) {
    super(ApplicationConstants.Environment.CONTAINER_ID.name());

    this.amrmClient = AMRMClient.createAMRMClient();
    this.amrmClient.init(conf);
    this.nmClient = new Hadoop21YarnNMClient(conf);
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
    nmClient.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    nmClient.stopAndWait();
    amrmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, null, trackerUrl.toString());
    amrmClient.stop();
  }

  @Override
  public String getHost() {
    return System.getenv().get(ApplicationConstants.Environment.NM_HOST.name());
  }

  @Override
  public int getNMPort() {
    return Integer.parseInt(System.getenv().get(ApplicationConstants.Environment.NM_PORT.name()));
  }

  @Override
  protected AMRMClient.ContainerRequest createContainerRequest(Priority priority, Resource capability,
                                                               @Nullable String[] hosts, @Nullable String[] racks,
                                                               boolean relaxLocality) {
    return new AMRMClient.ContainerRequest(capability, hosts, racks, priority, relaxLocality);
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
    // An empty implementation since Blacklist is not supported in Hadoop-2.1 AMRMClient.
    if (recordUnsupportedFeature("blacklist")) {
      LOG.warn("Blacklist is not supported in Hadoop 2.1 AMRMClient");
    }
  }

  @Override
  protected AllocateResult doAllocate(float progress) throws Exception {
    AllocateResponse allocateResponse = amrmClient.allocate(progress);
    List<RunnableProcessLauncher> launchers
      = Lists.newArrayListWithCapacity(allocateResponse.getAllocatedContainers().size());

    for (Container container : allocateResponse.getAllocatedContainers()) {
      launchers.add(new RunnableProcessLauncher(new Hadoop21YarnContainerInfo(container), nmClient));
    }

    List<YarnContainerStatus> completed = ImmutableList.copyOf(
      Iterables.transform(allocateResponse.getCompletedContainersStatuses(), STATUS_TRANSFORM));

    return new AllocateResult(launchers, completed);
  }

  @Override
  protected void releaseAssignedContainer(YarnContainerInfo containerInfo) {
    Container container = containerInfo.getContainer();
    amrmClient.releaseAssignedContainer(container.getId());
  }

  @Override
  protected Resource adjustCapability(Resource resource) {
    int cores = resource.getVirtualCores();
    int updatedCores = Math.min(resource.getVirtualCores(), maxCapability.getVirtualCores());

    if (cores != updatedCores) {
      resource.setVirtualCores(updatedCores);
      LOG.info("Adjust virtual cores requirement from {} to {}.", cores, updatedCores);
    }

    int updatedMemory = Math.min(resource.getMemory(), maxCapability.getMemory());
    if (resource.getMemory() != updatedMemory) {
      LOG.info("Adjust memory requirement from {} to {} MB.", resource.getMemory(), updatedMemory);
      resource.setMemory(updatedMemory);
    }

    return resource;
  }
}
