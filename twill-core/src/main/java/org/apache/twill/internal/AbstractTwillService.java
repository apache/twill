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

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import org.apache.twill.api.RunId;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.state.MessageCallback;
import org.apache.twill.internal.state.MessageCodec;
import org.apache.twill.internal.state.SystemMessages;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A base implementation of {@link Service} that uses ZooKeeper to transmit states and messages. It uses
 * the following directory structure in ZK:
 *
 * <pre>
 * /instances
 *     |- [runId_1]
 *     |- [runId_2]
 *     |- ...
 * /[runId_1]
 *     |- messages
 *          |- [messageId_1]
 *          |- [messageId_2]
 *          |- ....
 * /[runId_2]
 *     |- messages
 * </pre>
 *
 * It assumes that the zk root node is already namespaced
 * (either with applicationId for AM or runnableId for containers).
 * <p/>
 * The ephemeral nodes under {@code /instances} are the {@code liveNode} for each running instance. It can carries data
 * about that service, which is set by the corresponding implementation.
 * <p/>
 * Each running instance also has its own node named by the runId. Under that node, it has a {@code messages} node for
 * receiving messages from the controller. New message is created by creating a sequence node under the {@code messages}
 * node, with the node data carrying the message content. The message node will be removed once the message
 * is being processed by the service.
 */
public abstract class AbstractTwillService extends AbstractExecutionThreadService implements MessageCallback {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractTwillService.class);
  private static final Gson GSON = new GsonBuilder().serializeNulls().create();

  protected final ZKClient zkClient;
  protected final RunId runId;
  private ExecutorService messageCallbackExecutor;
  private Cancellable watcherCancellable;

  protected AbstractTwillService(final ZKClient zkClient, RunId runId) {
    this.zkClient = zkClient;
    this.runId = runId;
  }

  /**
   * Override to perform any work during service start.
   */
  protected void doStart() throws Exception {
    // Default no-op
  }

  /**
   * Override to execution service work. When this method returns, this Service will stop.
   */
  protected void doRun() throws Exception {
    // Default no-op
  }

  /**
   * Overrides to perform any work during service shutdown.
   */
  protected void doStop() throws Exception {
    // Default no-op
  }

  /**
   * Returns an Object to be stored in the live node. The object return will be GSon serialized. If {@code null}
   * is returned, no data will be stored to the live node.
   */
  protected Object getLiveNodeData() {
    return null;
  }

  /**
   * Returns a {@link Gson} instance for serializing object returned by the {@link #getLiveNodeData()} method.
   */
  protected Gson getLiveNodeGson() {
    return GSON;
  }

  /**
   * Handles message by simply logging it. Child class should override this method for custom handling of message.
   *
   * @see org.apache.twill.internal.state.MessageCallback
   */
  @Override
  public ListenableFuture<String> onReceived(String messageId, Message message) {
    LOG.info("Message received: {}", message);
    return Futures.immediateCheckedFuture(messageId);
  }

  @Override
  protected final void startUp() throws Exception {
    // Single thread executor that will discard task silently if it is already terminated, which only
    // happens when this service is shutting down.
    messageCallbackExecutor = new ThreadPoolExecutor(1, 1,
                                                     0L, TimeUnit.MILLISECONDS,
                                                     new LinkedBlockingQueue<Runnable>(),
                                                     Threads.createDaemonThreadFactory("message-callback"),
                                                     new ThreadPoolExecutor.DiscardPolicy());

    // Watch for session expiration, recreate the live node if reconnected after expiration.
    watcherCancellable = zkClient.addConnectionWatcher(new Watcher() {
      private boolean expired = false;

      @Override
      public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.Expired) {
          LOG.warn("ZK Session expired for service {} with runId {}.", getServiceName(), runId.getId());
          expired = true;
        } else if (event.getState() == Event.KeeperState.SyncConnected && expired) {
          LOG.info("Reconnected after expiration for service {} with runId {}", getServiceName(), runId.getId());
          expired = false;
          logIfFailed(createLiveNode());
        }
      }
    });

    // Create the live node, if succeeded, start the service, otherwise fail out.
    createLiveNode().get();

    // Create node for messaging
    ZKOperations.ignoreError(zkClient.create(getZKPath("messages"), null, CreateMode.PERSISTENT),
                             KeeperException.NodeExistsException.class, null).get();

    doStart();

    // Starts watching for messages
    watchMessages();
  }

  @Override
  protected final void run() throws Exception {
    doRun();
  }

  @Override
  protected final void shutDown() throws Exception {
    if (watcherCancellable != null) {
      watcherCancellable.cancel();
    }

    messageCallbackExecutor.shutdownNow();
    try {
      doStop();
    } finally {
      // Given at most 5 seconds to cleanup ZK nodes
      removeLiveNode().get(5, TimeUnit.SECONDS);
      LOG.info("Service {} with runId {} shutdown completed", getServiceName(), runId.getId());
    }
  }

  /**
   * Update the live node for the runnable.
   *
   * @return A {@link OperationFuture} that will be completed when the update is done.
   */
  protected final OperationFuture<?> updateLiveNode() {
    String liveNodePath = getLiveNodePath();
    LOG.info("Update live node {}{}", zkClient.getConnectString(), liveNodePath);
    return zkClient.setData(liveNodePath, serializeLiveNode());
  }

  private OperationFuture<String> createLiveNode() {
    String liveNodePath = getLiveNodePath();
    LOG.info("Create live node {}{}", zkClient.getConnectString(), liveNodePath);
    return ZKOperations.ignoreError(zkClient.create(liveNodePath, serializeLiveNode(), CreateMode.EPHEMERAL),
                                    KeeperException.NodeExistsException.class, liveNodePath);
  }

  private OperationFuture<String> removeLiveNode() {
    String liveNode = getLiveNodePath();
    LOG.info("Remove live node {}{}", zkClient.getConnectString(), liveNode);
    return ZKOperations.ignoreError(zkClient.delete(liveNode), KeeperException.NoNodeException.class, liveNode);
  }

  /**
   * Watches for messages that are sent through ZK messages node.
   */
  private void watchMessages() {
    final String messagesPath = getZKPath("messages");
    Futures.addCallback(zkClient.getChildren(messagesPath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (event.getType() == Event.EventType.NodeChildrenChanged && isRunning()) {
          watchMessages();
        }
      }
    }), new FutureCallback<NodeChildren>() {
      @Override
      public void onSuccess(NodeChildren result) {
        // Sort by the name, which is the messageId. Assumption is that message ids is ordered by time.
        List<String> messages = Lists.newArrayList(result.getChildren());
        Collections.sort(messages);
        for (String messageId : messages) {
          processMessage(messagesPath + "/" + messageId, messageId);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // TODO: what could be done besides just logging?
        LOG.error("Failed to watch messages.", t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private void processMessage(final String path, final String messageId) {
    Futures.addCallback(zkClient.getData(path), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        Runnable messageRemover = createMessageRemover(path, result.getStat().getVersion());

        Message message = MessageCodec.decode(result.getData());
        if (message == null) {
          LOG.error("Failed to decode message for {} in {}", messageId, path);
          messageRemover.run();
          return;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Message received from {}: {}", path, new String(MessageCodec.encode(message), Charsets.UTF_8));
        }

        // Handle the stop message
        if (handleStopMessage(message, messageRemover)) {
          return;
        }
        // Otherwise, delegate to the child class to handle the message
        handleMessage(messageId, message, messageRemover);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to fetch message content from {}", path, t);
      }
    }, messageCallbackExecutor);
  }

  /**
   * Handles {@link SystemMessages#STOP_COMMAND} if the given message is a stop command. After this service is stopped,
   * the message node will be removed.
   *
   * @param message Message to process
   * @param messageRemover Runnable to remove the message node when this service is stopped
   * @return {@code true} if the given message is a stop command, {@code false} otherwise
   */
  private boolean handleStopMessage(Message message, final Runnable messageRemover) {
    if (message.getType() != Message.Type.SYSTEM || !SystemMessages.STOP_COMMAND.equals(message.getCommand())) {
      return false;
    }

    // Stop this service.
    Futures.addCallback(stop(), new FutureCallback<State>() {
      @Override
      public void onSuccess(State result) {
        messageRemover.run();
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Stop service failed upon STOP command", t);
        messageRemover.run();
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return true;
  }


  /**
   * Handles the given message by calling {@link #onReceived(java.lang.String, org.apache.twill.internal.state.Message)}
   * method.
   *
   * @param messageId Id of the message
   * @param message The message
   * @param messageRemover Runnable to remove the message node when the handling of the message is completed
   */
  private void handleMessage(String messageId, final Message message, final Runnable messageRemover) {
    Futures.addCallback(onReceived(messageId, message), new FutureCallback<String>() {
      @Override
      public void onSuccess(String result) {
        messageRemover.run();
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to handle message {}", message, t);
        messageRemover.run();
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  /**
   * Creates a {@link Runnable} that encapsulation the action to remove a particular message node.
   */
  private Runnable createMessageRemover(final String path, final int version) {
    return new Runnable() {
      @Override
      public void run() {
        logIfFailed(zkClient.delete(path, version));
      }
    };
  }

  /**
   * Logs if the given future failed.
   */
  private <T> void logIfFailed(ListenableFuture<T> future) {
    Futures.addCallback(future, new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        // All-good
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Operation failed for service {} with runId {}", getServiceName(), runId, t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private String getZKPath(String path) {
    return String.format("/%s/%s", runId.getId(), path);
  }

  private String getLiveNodePath() {
    return String.format("%s/%s", Constants.INSTANCES_PATH_PREFIX, runId.getId());
  }

  private byte[] serializeLiveNode() {
    JsonObject content = new JsonObject();
    Object liveNodeData = getLiveNodeData();
    if (liveNodeData != null) {
      content.add("data", getLiveNodeGson().toJsonTree(liveNodeData));
    }
    return GSON.toJson(content).getBytes(Charsets.UTF_8);
  }
}
