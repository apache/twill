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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Table;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceController;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.internal.ContainerExitCodes;
import org.apache.twill.internal.ContainerInfo;
import org.apache.twill.internal.ContainerLiveNodeData;
import org.apache.twill.internal.DefaultResourceReport;
import org.apache.twill.internal.DefaultTwillRunResources;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.TwillContainerController;
import org.apache.twill.internal.TwillContainerLauncher;
import org.apache.twill.internal.container.TwillContainerMain;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.yarn.YarnContainerStatus;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A helper class for ApplicationMasterService to keep track of running containers and to interact
 * with them.
 */
final class RunningContainers {
  private static final Logger LOG = LoggerFactory.getLogger(RunningContainers.class);

  /**
   * Function to return cardinality of a given BitSet.
   */
  private static final Function<BitSet, Integer> BITSET_CARDINALITY = new Function<BitSet, Integer>() {
    @Override
    public Integer apply(BitSet input) {
      return input.cardinality();
    }
  };

  // Table of <runnableName, containerId, controller>
  private final Table<String, String, TwillContainerController> containers;

  // Map from runnableName to a BitSet, with the <instanceId> bit turned on for having an instance running.
  private final Map<String, BitSet> runnableInstances;
  private final Map<String, Integer> completedContainerCount;
  private final DefaultResourceReport resourceReport;
  private final Deque<String> startSequence;
  private final Lock containerLock;
  private final Condition containerChange;
  private final ZKClient zkClient;
  private final Multimap<String, ContainerInfo> containerStats;

  RunningContainers(String appId, TwillRunResources appMasterResources, ZKClient zookeeperClient) {
    containers = HashBasedTable.create();
    runnableInstances = Maps.newHashMap();
    completedContainerCount = Maps.newHashMap();
    startSequence = Lists.newLinkedList();
    containerLock = new ReentrantLock();
    containerChange = containerLock.newCondition();
    resourceReport = new DefaultResourceReport(appId, appMasterResources);
    zkClient = zookeeperClient;
    containerStats = HashMultimap.create();
  }

  /**
   * Returns {@code true} if there is no live container.
   */
  boolean isEmpty() {
    containerLock.lock();
    try {
      return runnableInstances.isEmpty();
    } finally {
      containerLock.unlock();
    }
  }

  void start(String runnableName, ContainerInfo containerInfo, TwillContainerLauncher launcher) {
    containerLock.lock();
    try {
      int instanceId = getStartInstanceId(runnableName);
      RunId runId = getRunId(runnableName, instanceId);
      TwillContainerController controller = launcher.start(runId, instanceId,
                                                           TwillContainerMain.class, "$HADOOP_CONF_DIR");
      containers.put(runnableName, containerInfo.getId(), controller);
      TwillRunResources resources = new DynamicTwillRunResources(instanceId,
                                                                 containerInfo.getId(),
                                                                 containerInfo.getVirtualCores(),
                                                                 containerInfo.getMemoryMB(),
                                                                 containerInfo.getHost().getHostName(),
                                                                 controller);
      resourceReport.addRunResources(runnableName, resources);
      containerStats.put(runnableName, containerInfo);

      if (startSequence.isEmpty() || !runnableName.equals(startSequence.peekLast())) {
        startSequence.addLast(runnableName);
      }
      containerChange.signalAll();

    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Watch for changes to services under given path.
   * @param path to check for changes.
   */
  void addWatcher(String path) {
    ZKOperations.watchChildren(zkClient, path, new ZKOperations.ChildrenCallback() {
      @Override
      public void updated(NodeChildren nodeChildren) {
        resourceReport.setServices(nodeChildren.getChildren());
      }
    });
  }

  ResourceReport getResourceReport() {
    return resourceReport;
  }

  /**
   * Given a runnable name, returns a list of {@link org.apache.twill.internal.ContainerInfo} for it's instances.
   * @param runnableName name of a runnable.
   * @return a list of {@link org.apache.twill.internal.ContainerInfo} for instances of a runnable.
   */
  Collection<ContainerInfo> getContainerInfo(String runnableName) {
    return containerStats.get(runnableName);
  }

  /**
   * Stops and removes the last running container of the given runnable.
   */
  void removeLast(String runnableName) {
    containerLock.lock();
    try {
      int maxInstanceId = getMaxInstanceId(runnableName);
      if (maxInstanceId < 0) {
        LOG.warn("No running container found for {}", runnableName);
        return;
      }

      String lastContainerId = null;
      TwillContainerController lastController = null;

      // Find the controller with the maxInstanceId
      for (Map.Entry<String, TwillContainerController> entry : containers.row(runnableName).entrySet()) {
        if (getInstanceId(entry.getValue().getRunId()) == maxInstanceId) {
          lastContainerId = entry.getKey();
          lastController = entry.getValue();
          break;
        }
      }

      Preconditions.checkState(lastContainerId != null,
                               "No container found for {} with instanceId = {}", runnableName, maxInstanceId);

      LOG.info("Stopping service: {} {}", runnableName, lastController.getRunId());
      lastController.stopAndWait();
      containers.remove(runnableName, lastContainerId);
      removeContainerInfo(lastContainerId);
      removeInstanceId(runnableName, maxInstanceId);
      resourceReport.removeRunnableResources(runnableName, lastContainerId);
      containerChange.signalAll();
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Blocks until there are changes in running containers.
   */
  void waitForCount(String runnableName, int count) throws InterruptedException {
    containerLock.lock();
    try {
      while (getRunningInstances(runnableName) != count) {
        containerChange.await();
      }
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Returns the number of running instances of the given runnable.
   */
  int count(String runnableName) {
    containerLock.lock();
    try {
      return getRunningInstances(runnableName);
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Returns a Map contains running instances of all runnables.
   */
  Map<String, Integer> countAll() {
    containerLock.lock();
    try {
      return ImmutableMap.copyOf(Maps.transformValues(runnableInstances, BITSET_CARDINALITY));
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Returns a Map containing number of successfully completed containers for all runnables.
   */
  Map<String, Integer> getCompletedContainerCount() {
    containerLock.lock();
    try {
      return ImmutableMap.copyOf(completedContainerCount);
    } finally {
      containerLock.unlock();
    }
  }

  void sendToAll(Message message, Runnable completion) {
    containerLock.lock();
    try {
      if (containers.isEmpty()) {
        completion.run();
      }

      // Sends the command to all running containers
      AtomicInteger count = new AtomicInteger(containers.size());
      for (Map.Entry<String, Map<String, TwillContainerController>> entry : containers.rowMap().entrySet()) {
        for (TwillContainerController controller : entry.getValue().values()) {
          sendMessage(entry.getKey(), message, controller, count, completion);
        }
      }
    } finally {
      containerLock.unlock();
    }
  }

  void sendToRunnable(String runnableName, Message message, Runnable completion) {
    containerLock.lock();
    try {
      Collection<TwillContainerController> controllers = containers.row(runnableName).values();
      if (controllers.isEmpty()) {
        completion.run();
      }

      AtomicInteger count = new AtomicInteger(controllers.size());
      for (TwillContainerController controller : controllers) {
        sendMessage(runnableName, message, controller, count, completion);
      }
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Stops all running services. Only called when the AppMaster stops.
   */
  void stopAll() {
    containerLock.lock();
    try {
      // Stop it one by one in reverse order of start sequence
      Iterator<String> itor = startSequence.descendingIterator();
      List<ListenableFuture<ServiceController.State>> futures = Lists.newLinkedList();
      while (itor.hasNext()) {
        String runnableName = itor.next();
        LOG.info("Stopping all instances of " + runnableName);

        futures.clear();
        // Parallel stops all running containers of the current runnable.
        for (TwillContainerController controller : containers.row(runnableName).values()) {
          futures.add(controller.stop());
        }
        // Wait for containers to stop. Assumes the future returned by Futures.successfulAsList won't throw exception.
        Futures.getUnchecked(Futures.successfulAsList(futures));

        LOG.info("Terminated all instances of " + runnableName);
      }
      containers.clear();
      runnableInstances.clear();
      containerStats.clear();
    } finally {
      containerLock.unlock();
    }
  }

  Set<String> getContainerIds() {
    containerLock.lock();
    try {
      return ImmutableSet.copyOf(containers.columnKeySet());
    } finally {
      containerLock.unlock();
    }
  }

  /**
   * Handle completion of container.
   *
   * @param status           The completion status.
   * @param restartRunnables Set of runnable names that requires restart.
   */
  void handleCompleted(YarnContainerStatus status, Multiset<String> restartRunnables) {
    containerLock.lock();
    String containerId = status.getContainerId();
    int exitStatus = status.getExitStatus();
    ContainerState state = status.getState();

    try {
      removeContainerInfo(containerId);
      Map<String, TwillContainerController> lookup = containers.column(containerId);
      if (lookup.isEmpty()) {
        // It's OK because if a container is stopped through removeLast, this would be empty.
        return;
      }

      if (lookup.size() != 1) {
        LOG.warn("More than one controller found for container {}", containerId);
      }

      if (exitStatus != ContainerExitCodes.SUCCESS) {
        LOG.warn("Container {} exited abnormally with state {}, exit code {}.",
                 containerId, state, exitStatus);
        if (shouldRetry(exitStatus)) {
          LOG.info("Re-request the container {} for exit code {}.", containerId, exitStatus);
          restartRunnables.add(lookup.keySet().iterator().next());
        }
      } else {
        LOG.info("Container {} exited normally with state {}", containerId, state);
      }

      for (Map.Entry<String, TwillContainerController> completedEntry : lookup.entrySet()) {
        String runnableName = completedEntry.getKey();
        TwillContainerController controller = completedEntry.getValue();
        controller.completed(exitStatus);

        if (exitStatus == ContainerExitCodes.SUCCESS) {
          if (!completedContainerCount.containsKey(runnableName)) {
            completedContainerCount.put(runnableName, 0);
          }
          completedContainerCount.put(runnableName, completedContainerCount.get(runnableName) + 1);
        }
        removeInstanceId(runnableName, getInstanceId(controller.getRunId()));
        resourceReport.removeRunnableResources(runnableName, containerId);
      }

      lookup.clear();
      containerChange.signalAll();
    } finally {
      containerLock.unlock();
    }
  }

  private boolean shouldRetry(int exitCode) {
    return exitCode != ContainerExitCodes.SUCCESS
      && exitCode != ContainerExitCodes.DISKS_FAILED
      && exitCode != ContainerExitCodes.INIT_FAILED;
  }

  /**
   * Sends a command through the given {@link TwillContainerController} of a runnable. Decrements the count
   * when the sending of command completed. Triggers completion when count reaches zero.
   */
  private void sendMessage(final String runnableName, final Message message,
                           final TwillContainerController controller, final AtomicInteger count,
                           final Runnable completion) {
    Futures.addCallback(controller.sendMessage(message), new FutureCallback<Message>() {
      @Override
      public void onSuccess(Message result) {
        if (count.decrementAndGet() == 0) {
          completion.run();
        }
      }

      @Override
      public void onFailure(Throwable t) {
        try {
          LOG.error("Failed to send message. Runnable: {}, RunId: {}, Message: {}.",
                    runnableName, controller.getRunId(), message, t);
        } finally {
          if (count.decrementAndGet() == 0) {
            completion.run();
          }
        }
      }
    });
  }

  /**
   * Returns the instanceId to start the given runnable.
   */
  private int getStartInstanceId(String runnableName) {
    BitSet instances = runnableInstances.get(runnableName);
    if (instances == null) {
      instances = new BitSet();
      runnableInstances.put(runnableName, instances);
    }
    int instanceId = instances.nextClearBit(0);
    instances.set(instanceId);
    return instanceId;
  }

  private void removeInstanceId(String runnableName, int instanceId) {
    BitSet instances = runnableInstances.get(runnableName);
    if (instances == null) {
      return;
    }
    instances.clear(instanceId);
    if (instances.isEmpty()) {
      runnableInstances.remove(runnableName);
    }
  }

  /**
   * Returns the largest instanceId for the given runnable. Returns -1 if no container is running.
   */
  private int getMaxInstanceId(String runnableName) {
    BitSet instances = runnableInstances.get(runnableName);
    if (instances == null || instances.isEmpty()) {
      return -1;
    }
    return instances.length() - 1;
  }

  /**
   * Returns nnumber of running instances for the given runnable.
   */
  private int getRunningInstances(String runableName) {
    BitSet instances = runnableInstances.get(runableName);
    return instances == null ? 0 : instances.cardinality();
  }

  private RunId getRunId(String runnableName, int instanceId) {
    RunId baseId;

    Collection<TwillContainerController> controllers = containers.row(runnableName).values();
    if (controllers.isEmpty()) {
      baseId = RunIds.generate();
    } else {
      String id = controllers.iterator().next().getRunId().getId();
      baseId = RunIds.fromString(id.substring(0, id.lastIndexOf('-')));
    }

    return RunIds.fromString(baseId.getId() + '-' + instanceId);
  }

  private int getInstanceId(RunId runId) {
    String id = runId.getId();
    return Integer.parseInt(id.substring(id.lastIndexOf('-') + 1));
  }

  /**
   * Given the containerId, removes the corresponding containerInfo.
   * @param containerId Id for the container to be removed.
   * @return Returns {@code false} if container with the provided id was not found, {@code true} otherwise.
   */
  private boolean removeContainerInfo(String containerId) {
    for (ContainerInfo containerInfo : this.containerStats.values()) {
      if (containerInfo.getId().equals(containerId)) {
        this.containerStats.values().remove(containerInfo);
        return true;
      }
    }
    return false;
  }

  /**
   * A helper class that overrides the debug port of the resources with the live info from the container controller.
   */
  private static class DynamicTwillRunResources extends DefaultTwillRunResources {

    private final TwillContainerController controller;
    private Integer dynamicDebugPort = null;

    private DynamicTwillRunResources(int instanceId, String containerId,
                                     int cores, int memoryMB, String host,
                                     TwillContainerController controller) {
      super(instanceId, containerId, cores, memoryMB, host, null);
      this.controller = controller;
    }

    @Override
    public synchronized Integer getDebugPort() {
      if (dynamicDebugPort == null) {
        ContainerLiveNodeData liveData = controller.getLiveNodeData();
        if (liveData != null && liveData.getDebugPort() != null) {
          try {
            dynamicDebugPort = Integer.parseInt(liveData.getDebugPort());
          } catch (NumberFormatException e) {
            LOG.warn("Live data for {} has debug port of '{}' which cannot be parsed as a number",
                     getContainerId(), liveData.getDebugPort());
          }
        }
      }
      return dynamicDebugPort;
    }
  }
}
