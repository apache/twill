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
package org.apache.twill.internal;

import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.ResourceReporter;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.internal.zookeeper.ReentrantDistributedLock;
import org.apache.twill.zookeeper.ZKClient;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nullable;

/**
 * Implementation of {@link TwillContext} that provides the basic runtime information of a {@link TwillRunnable}.
 */
public final class BasicTwillContext implements TwillContext {

  private final RunId runId;
  private final RunId appRunId;
  private final InetAddress host;
  private final String[] args;
  private final String[] appArgs;
  private final TwillRunnableSpecification spec;
  private final int instanceId;
  private final DiscoveryService discoveryService;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final int allowedMemoryMB;
  private final int virtualCores;
  private final ZKClient zkClient;
  private final ElectionRegistry elections;
  private final ResourceReporter resourceReporter;
  private volatile int instanceCount;

  public BasicTwillContext(RunId runId, RunId appRunId, InetAddress host, String[] args, String[] appArgs,
                           TwillRunnableSpecification spec, int instanceId,
                           DiscoveryService discoveryService, DiscoveryServiceClient discoveryServiceClient,
                           ZKClient zkClient, int instanceCount, int allowedMemoryMB, int virtualCores,
                           URL trackerURL) {
    this.runId = runId;
    this.appRunId = appRunId;
    this.host = host;
    this.args = args;
    this.appArgs = appArgs;
    this.spec = spec;
    this.instanceId = instanceId;
    this.discoveryService = discoveryService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.zkClient = zkClient;
    this.elections = new ElectionRegistry(zkClient);
    this.instanceCount = instanceCount;
    this.allowedMemoryMB = allowedMemoryMB;
    this.virtualCores = virtualCores;
    ResourceReporter resourceReporter;
    try {
      resourceReporter = new ResourceReportClient(Collections.singletonList(
        new URL(trackerURL.getProtocol(), trackerURL.getHost(),
                trackerURL.getPort(), trackerURL.getPath() + Constants.TRACKER_SERVICE_BASE_URI)));
    } catch (MalformedURLException e) {
      // This shouldn't happen. If it does, use a noop resource reporter
      resourceReporter = new NoopResourceReporter();
    }
    this.resourceReporter = resourceReporter;
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public RunId getApplicationRunId() {
    return appRunId;
  }

  @Override
  public int getInstanceCount() {
    return instanceCount;
  }

  public void setInstanceCount(int count) {
    this.instanceCount = count;
  }

  @Override
  public InetAddress getHost() {
    return host;
  }

  @Override
  public String[] getArguments() {
    return args;
  }

  @Override
  public String[] getApplicationArguments() {
    return appArgs;
  }

  @Override
  public TwillRunnableSpecification getSpecification() {
    return spec;
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  @Override
  public int getVirtualCores() {
    return virtualCores;
  }

  @Override
  public int getMaxMemoryMB() {
    return allowedMemoryMB;
  }

  @Override
  public Cancellable announce(final String serviceName, final int port) {
    return announce(serviceName, port, new byte[]{});
  }

  @Override
  public Cancellable announce(final String serviceName, final int port, final byte[] payload) {
    return discoveryService.register(new Discoverable(serviceName, new InetSocketAddress(getHost(), port), payload));
  }

  @Override
  public ServiceDiscovered discover(String name) {
    return discoveryServiceClient.discover(name);
  }

  @Override
  public Cancellable electLeader(String name, ElectionHandler participantHandler) {
    return elections.register("/leader/" + name, participantHandler);
  }

  @Override
  public Lock createLock(String name) {
    return new ReentrantDistributedLock(zkClient, "/lock/" + name);
  }

  /**
   * Stops and frees any currently allocated resources.
   */
  public void stop() {
    elections.shutdown();
  }

  @Nullable
  @Override
  public ResourceReport getResourceReport() {
    return resourceReporter.getResourceReport();
  }

  @Nullable
  @Override
  public TwillRunResources getApplicationMasterResources() {
    return resourceReporter.getApplicationMasterResources();
  }

  @Override
  public Map<String, Collection<TwillRunResources>> getRunnablesResources() {
    return resourceReporter.getRunnablesResources();
  }

  @Override
  public Collection<TwillRunResources> getInstancesResources(String runnableName) {
    return resourceReporter.getInstancesResources(runnableName);
  }
}
