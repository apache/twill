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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.state.StateNode;
import org.apache.twill.launcher.TwillLauncher;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This class helps launching a container.
 */
public final class TwillContainerLauncher {

  private static final Logger LOG = LoggerFactory.getLogger(TwillContainerLauncher.class);

  private static final double HEAP_MIN_RATIO = 0.7d;

  private final RuntimeSpecification runtimeSpec;
  private final ProcessLauncher.PrepareLaunchContext launchContext;
  private final ZKClient zkClient;
  private final int instanceCount;
  private final String jvmOpts;
  private final int reservedMemory;
  private final Location secureStoreLocation;

  public TwillContainerLauncher(RuntimeSpecification runtimeSpec, ProcessLauncher.PrepareLaunchContext launchContext,
                                ZKClient zkClient, int instanceCount, String jvmOpts, int reservedMemory,
                                Location secureStoreLocation) {
    this.runtimeSpec = runtimeSpec;
    this.launchContext = launchContext;
    this.zkClient = zkClient;
    this.instanceCount = instanceCount;
    this.jvmOpts = jvmOpts;
    this.reservedMemory = reservedMemory;
    this.secureStoreLocation = secureStoreLocation;
  }

  public TwillContainerController start(RunId runId, int instanceId, Class<?> mainClass, String classPath) {
    ProcessLauncher.PrepareLaunchContext.AfterResources afterResources = null;
    ProcessLauncher.PrepareLaunchContext.ResourcesAdder resourcesAdder = null;

    // Clean up zookeeper path in case this is a retry and there are old messages and state there.
    Futures.getUnchecked(ZKOperations.ignoreError(
      ZKOperations.recursiveDelete(zkClient, "/" + runId), KeeperException.NoNodeException.class, null));

    // Adds all file to be localized to container
    if (!runtimeSpec.getLocalFiles().isEmpty()) {
      resourcesAdder = launchContext.withResources();

      for (LocalFile localFile : runtimeSpec.getLocalFiles()) {
        afterResources = resourcesAdder.add(localFile);
      }
    }

    // Optionally localize secure store.
    try {
      if (secureStoreLocation != null && secureStoreLocation.exists()) {
        if (resourcesAdder == null) {
          resourcesAdder = launchContext.withResources();
        }
        afterResources = resourcesAdder.add(new DefaultLocalFile(Constants.Files.CREDENTIALS,
                                                                 secureStoreLocation.toURI(),
                                                                 secureStoreLocation.lastModified(),
                                                                 secureStoreLocation.length(), false, null));
      }
    } catch (IOException e) {
      LOG.warn("Failed to launch container with secure store {}.", secureStoreLocation.toURI());
    }

    if (afterResources == null) {
      afterResources = launchContext.noResources();
    }

    int memory = runtimeSpec.getResourceSpecification().getMemorySize();
    if (((double) (memory - reservedMemory) / memory) >= HEAP_MIN_RATIO) {
      // Reduce -Xmx by the reserved memory size.
      memory = runtimeSpec.getResourceSpecification().getMemorySize() - reservedMemory;
    } else {
      // If it is a small VM, just discount it by the min ratio.
      memory = (int) Math.ceil(memory * HEAP_MIN_RATIO);
    }

    // Currently no reporting is supported for runnable containers
    ProcessController<Void> processController = afterResources
      .withEnvironment()
      .add(EnvKeys.TWILL_RUN_ID, runId.getId())
      .add(EnvKeys.TWILL_RUNNABLE_NAME, runtimeSpec.getName())
      .add(EnvKeys.TWILL_INSTANCE_ID, Integer.toString(instanceId))
      .add(EnvKeys.TWILL_INSTANCE_COUNT, Integer.toString(instanceCount))
      .withCommands()
      .add("java",
           "-Djava.io.tmpdir=tmp",
           "-Dyarn.container=$" + EnvKeys.YARN_CONTAINER_ID,
           "-Dtwill.runnable=$" + EnvKeys.TWILL_APP_NAME + ".$" + EnvKeys.TWILL_RUNNABLE_NAME,
           "-cp", Constants.Files.LAUNCHER_JAR + ":" + classPath,
           "-Xmx" + memory + "m",
           jvmOpts,
           TwillLauncher.class.getName(),
           Constants.Files.CONTAINER_JAR,
           mainClass.getName(),
           Boolean.TRUE.toString())
      .redirectOutput(Constants.STDOUT).redirectError(Constants.STDERR)
      .launch();

    TwillContainerControllerImpl controller = new TwillContainerControllerImpl(zkClient, runId, processController);
    controller.start();
    return controller;
  }

  private static final class TwillContainerControllerImpl extends AbstractZKServiceController
                                                          implements TwillContainerController {

    private final ProcessController<Void> processController;

    protected TwillContainerControllerImpl(ZKClient zkClient, RunId runId,
                                           ProcessController<Void> processController) {
      super(runId, zkClient);
      this.processController = processController;
    }

    @Override
    protected void doStartUp() {
      // No-op
    }

    @Override
    protected void doShutDown() {
      // No-op
    }

    @Override
    protected void instanceNodeUpdated(NodeData nodeData) {
      // No-op
    }

    @Override
    protected void instanceNodeFailed(Throwable cause) {
      // No-op
    }

    @Override
    protected void stateNodeUpdated(StateNode stateNode) {
      // No-op
    }

    @Override
    public ListenableFuture<Message> sendMessage(Message message) {
      return sendMessage(message, message);
    }

    @Override
    public synchronized void completed(int exitStatus) {
      if (exitStatus != 0) {  // If a container terminated with exit code != 0, treat it as error
//        fireStateChange(new StateNode(State.FAILED, new StackTraceElement[0]));
      }
      forceShutDown();
    }

    @Override
    public void kill() {
      processController.cancel();
    }
  }
}
