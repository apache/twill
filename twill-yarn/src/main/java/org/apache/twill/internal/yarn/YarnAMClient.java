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

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Service;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.twill.internal.ProcessLauncher;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * This interface provides abstraction for AM to interacts with YARN to abstract out YARN version specific
 * code, making multi-version compatibility easier.
 */
public interface YarnAMClient extends Service {

  /**
   * Builder for creating a container request.
   */
  abstract class ContainerRequestBuilder {

    protected final Resource capability;
    protected final int count;
    protected final Set<String> hosts = Sets.newHashSet();
    protected final Set<String> racks = Sets.newHashSet();
    protected final Priority priority = Records.newRecord(Priority.class);
    protected boolean relaxLocality;

    protected ContainerRequestBuilder(Resource capability, int count) {
      this.capability = capability;
      this.count = count;
      this.relaxLocality = true;
    }

    public ContainerRequestBuilder addHosts(Collection<String> newHosts) {
      return add(hosts, newHosts);
    }

    public ContainerRequestBuilder addRacks(Collection<String> newRacks) {
      return add(racks, newRacks);
    }

    public ContainerRequestBuilder setPriority(int prio) {
      priority.setPriority(prio);
      return this;
    }

    public ContainerRequestBuilder setRelaxLocality(boolean relaxLocality) {
      this.relaxLocality = relaxLocality;
      return this;
    }

    /**
     * Adds a container request. Returns an unique ID for the request.
     */
    public abstract String apply();

    private <T> ContainerRequestBuilder add(Collection<T> collection, Collection<T> more) {
      if (more != null) {
        collection.addAll(more);
      }
      return this;
    }
  }

  /**
   * Returns the container ID of the application.
   */
  ContainerId getContainerId();

  /**
   * Returns the hostname of the node manager that the AM is running on
   */
  String getHost();

  /**
   * Returns the port of the node manager that the AM is running on
   */
  int getNMPort();

  /**
   * Sets the tracker address and tracker url. This method should be called before calling {@link #start()}.
   */
  void setTracker(InetSocketAddress trackerAddr, URL trackerUrl);

  /**
   * The heartbeat call to RM.
   */
  void allocate(float progress, AllocateHandler handler) throws Exception;

  ContainerRequestBuilder addContainerRequest(Resource capability, int count);

  void addToBlacklist(String resource);

  void removeFromBlacklist(String resource);

  void clearBlacklist();

  /**
   * Notify a container request is fulfilled.
   *
   * Note: This method is needed to workaround a seemingly bug from AMRMClient implementation in YARN that if
   * a container is requested after a previous container was acquired (with the same capability), multiple containers
   * will get allocated instead of one.
   *
   * @param id The ID returned by {@link YarnAMClient.ContainerRequestBuilder#apply()}.
   */
  void completeContainerRequest(String id);

  /**
   * Callback for allocate call.
   */
  interface AllocateHandler {

    /**
     * Invokes when a list of containers has been acquired from YARN
     *
     * @param launchers list of launchers for launching runnables
     */
    void acquired(List<? extends ProcessLauncher<YarnContainerInfo>> launchers);

    /**
     * Invokes when containers completed
     *
     * @param completed list of completed container status
     */
    void completed(List<YarnContainerStatus> completed);
  }
}
