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

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.api.Command;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceController;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.state.Messages;
import org.apache.twill.internal.state.SystemMessages;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An abstract base class for implementing a {@link ServiceController} using ZooKeeper as a means for
 * communicating with the remote service. This is designed to work in pair with the {@link AbstractTwillService}.
 */
public abstract class AbstractZKServiceController extends AbstractExecutionServiceController {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractZKServiceController.class);

  protected final ZKClient zkClient;
  private final InstanceNodeDataCallback instanceNodeDataCallback;
  private final List<ListenableFuture<?>> messageFutures;
  private ListenableFuture<State> stopMessageFuture;

  protected AbstractZKServiceController(RunId runId, ZKClient zkClient) {
    super(runId);
    this.zkClient = zkClient;
    this.instanceNodeDataCallback = new InstanceNodeDataCallback();
    this.messageFutures = Lists.newLinkedList();
  }

  @Override
  public final ListenableFuture<Command> sendCommand(Command command) {
    return sendMessage(Messages.createForAll(command), command);
  }

  @Override
  public final ListenableFuture<Command> sendCommand(String runnableName, Command command) {
    return sendMessage(Messages.createForRunnable(runnableName, command), command);
  }

  @Override
  protected final void startUp() {
    doStartUp();

    // Watch for instance node existence.
    actOnExists(getInstancePath(), new Runnable() {
      @Override
      public void run() {
        watchInstanceNode();
      }
    });
  }

  @Override
  protected final synchronized void shutDown() {
    if (stopMessageFuture == null) {
      stopMessageFuture = ZKMessages.sendMessage(zkClient, getMessagePrefix(),
                                                 SystemMessages.stopApplication(), State.TERMINATED);
    }

    // Cancel all pending message futures.
    for (ListenableFuture<?> future : messageFutures) {
      future.cancel(true);
    }

    doShutDown();
  }

  /**
   * Sends a {@link Message} to the remote service. Returns a future that will be completed when the message
   * has been processed.
   * @param message The message to send.
   * @param result Object to set into the future when message is being processed.
   * @param <V> Type of the result.
   * @return A {@link ListenableFuture} that will be completed when the message has been processed.
   */
  protected final synchronized <V> ListenableFuture<V> sendMessage(Message message, V result) {
    if (!isRunning()) {
      return Futures.immediateFailedFuture(new IllegalStateException("Cannot send message to non-running application"));
    }
    final ListenableFuture<V> messageFuture = ZKMessages.sendMessage(zkClient, getMessagePrefix(), message, result);
    messageFutures.add(messageFuture);
    messageFuture.addListener(new Runnable() {
      @Override
      public void run() {
        // If the completion is triggered when stopping, do nothing.
        if (state() == State.STOPPING) {
          return;
        }
        synchronized (AbstractZKServiceController.this) {
          messageFutures.remove(messageFuture);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    return messageFuture;
  }

  protected final ListenableFuture<State> getStopMessageFuture() {
    return stopMessageFuture;
  }

  /**
   * Called during startup. Executed in the startup thread.
   */
  protected abstract void doStartUp();

  /**
   * Called during shutdown. Executed in the shutdown thread.
   */
  protected abstract void doShutDown();

  /**
   * Called when an update on the live instance node is detected.
   * @param nodeData The updated live instance node data or {@code null} if there is an error when fetching
   *                 the node data.
   */
  protected abstract void instanceNodeUpdated(NodeData nodeData);

  /**
   * Called when failed to fetch from live instance node.
   * @param cause The cause of failure of {@code null} if cause is unknown.
   */
  protected abstract void instanceNodeFailed(Throwable cause);

  protected synchronized void forceShutDown() {
    if (stopMessageFuture == null) {
      // In force shutdown, don't send message.
      stopMessageFuture = Futures.immediateFuture(State.TERMINATED);
    }
    stop();
  }


  private void actOnExists(final String path, final Runnable action) {
    // Watch for node existence.
    final AtomicBoolean nodeExists = new AtomicBoolean(false);
    Futures.addCallback(zkClient.exists(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!shouldProcessZKEvent()) {
          return;
        }
        // When node is created, call the action.
        // Other event type would be handled by the action.
        if (event.getType() == Event.EventType.NodeCreated && nodeExists.compareAndSet(false, true)) {
          action.run();
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result != null && nodeExists.compareAndSet(false, true)) {
          action.run();
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed in exists call to {}. Shutting down service.", path, t);
        forceShutDown();
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  protected final void watchInstanceNode() {
    if (!shouldProcessZKEvent()) {
      return;
    }
    Futures.addCallback(zkClient.getData(getInstancePath(), new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!shouldProcessZKEvent()) {
          return;
        }
        switch (event.getType()) {
          case NodeDataChanged:
            watchInstanceNode();
            break;
          case NodeDeleted:
            instanceNodeFailed(KeeperException.create(KeeperException.Code.NONODE, getInstancePath()));
            break;
          default:
            LOG.info("Ignore ZK event for instance node: {}", event);
        }
      }
    }), instanceNodeDataCallback, Threads.SAME_THREAD_EXECUTOR);
  }

  /**
   * Returns true if ZK events needs to be processed.
   */
  private boolean shouldProcessZKEvent() {
    State state = state();
    return (state == State.NEW || state == State.STARTING || state == State.RUNNING);
  }

  /**
   * Returns the path prefix for creating sequential message node for the remote service.
   */
  private String getMessagePrefix() {
    return getZKPath("messages/msg");
  }

  /**
   * Returns the zookeeper node path for the ephemeral instance node for this runId.
   */
  protected final String getInstancePath() {
    return String.format("%s/%s", Constants.INSTANCES_PATH_PREFIX, getRunId().getId());
  }

  private String getZKPath(String path) {
    return String.format("/%s/%s", getRunId().getId(), path);
  }

  private final class InstanceNodeDataCallback implements FutureCallback<NodeData> {

    @Override
    public void onSuccess(NodeData result) {
      if (shouldProcessZKEvent()) {
        instanceNodeUpdated(result);
      }
    }

    @Override
    public void onFailure(Throwable t) {
      LOG.error("Failed in fetching instance node data.", t);
      if (shouldProcessZKEvent()) {
        instanceNodeFailed(t);
      }
    }
  }
}
