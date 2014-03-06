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
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceController;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.json.StackTraceElementCodec;
import org.apache.twill.internal.json.StateNodeCodec;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.state.MessageCallback;
import org.apache.twill.internal.state.MessageCodec;
import org.apache.twill.internal.state.StateNode;
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
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;

/**
 * A {@link Service} decorator that wrap another {@link Service} with the service states reflected
 * to ZooKeeper.
 */
public final class ZKServiceDecorator extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(ZKServiceDecorator.class);

  private final ZKClient zkClient;
  private final RunId id;
  private final Supplier<? extends JsonElement> liveNodeData;
  private final Service decoratedService;
  private final MessageCallbackCaller messageCallback;
  private ExecutorService callbackExecutor;


  public ZKServiceDecorator(ZKClient zkClient, RunId id, Supplier<? extends JsonElement> liveNodeData,
                            Service decoratedService) {
    this(zkClient, id, liveNodeData, decoratedService, null);
  }

  /**
   * Creates a ZKServiceDecorator.
   * @param zkClient ZooKeeper client
   * @param id The run id of the service
   * @param liveNodeData A supplier for providing information writing to live node.
   * @param decoratedService The Service for monitoring state changes
   * @param finalizer An optional Runnable to run when this decorator terminated.
   */
  public ZKServiceDecorator(ZKClient zkClient, RunId id, Supplier <? extends JsonElement> liveNodeData,
                            Service decoratedService, @Nullable Runnable finalizer) {
    this.zkClient = zkClient;
    this.id = id;
    this.liveNodeData = liveNodeData;
    this.decoratedService = decoratedService;
    if (decoratedService instanceof MessageCallback) {
      this.messageCallback = new MessageCallbackCaller((MessageCallback) decoratedService, zkClient);
    } else {
      this.messageCallback = new MessageCallbackCaller(zkClient);
    }
    if (finalizer != null) {
      addFinalizer(finalizer);
    }
  }

  @Override
  protected void doStart() {
    callbackExecutor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("message-callback"));
    // Create the live node, if succeeded, start the decorated service, otherwise fail out.
    Futures.addCallback(createLiveNode(), new FutureCallback<String>() {
      @Override
      public void onSuccess(String result) {
        // Create nodes for states and messaging
        StateNode stateNode = new StateNode(ServiceController.State.STARTING);

        final ListenableFuture<List<String>> createFuture = Futures.allAsList(
          ZKOperations.ignoreError(zkClient.create(getZKPath("messages"), null, CreateMode.PERSISTENT),
                                   KeeperException.NodeExistsException.class, null),
          zkClient.create(getZKPath("state"), encodeStateNode(stateNode), CreateMode.PERSISTENT)
        );

        createFuture.addListener(new Runnable() {
          @Override
          public void run() {
            try {
              createFuture.get();
              // Starts the decorated service
              decoratedService.addListener(createListener(), Threads.SAME_THREAD_EXECUTOR);
              decoratedService.start();
            } catch (Exception e) {
              notifyFailed(e);
            }
          }
        }, Threads.SAME_THREAD_EXECUTOR);
      }

      @Override
      public void onFailure(Throwable t) {
        notifyFailed(t);
      }
    });

    // Watch for session expiration, recreate the live node if reconnected after expiration.
    zkClient.addConnectionWatcher(new Watcher() {
      private boolean expired = false;

      @Override
      public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.Expired) {
          LOG.warn("ZK Session expired for service {} with runId {}.", decoratedService, id.getId());
          expired = true;
        } else if (event.getState() == Event.KeeperState.SyncConnected && expired) {
          LOG.info("Reconnected after expiration for service {} with runId {}", decoratedService, id.getId());
          expired = false;
          Futures.addCallback(createLiveNode(), new FutureCallback<String>() {
            @Override
            public void onSuccess(String result) {
              // All good, no-op
            }

            @Override
            public void onFailure(Throwable t) {
              notifyFailed(t);
            }
          }, Threads.SAME_THREAD_EXECUTOR);
        }
      }
    });
  }

  @Override
  protected void doStop() {
    // Stops the decorated service
    decoratedService.stop();
    callbackExecutor.shutdownNow();
  }

  private void addFinalizer(final Runnable finalizer) {
    addListener(new ServiceListenerAdapter() {
      @Override
      public void terminated(State from) {
        try {
          finalizer.run();
        } catch (Throwable t) {
          LOG.warn("Exception when running finalizer.", t);
        }
      }

      @Override
      public void failed(State from, Throwable failure) {
        try {
          finalizer.run();
        } catch (Throwable t) {
          LOG.warn("Exception when running finalizer.", t);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private OperationFuture<String> createLiveNode() {
    String liveNode = getLiveNodePath();
    LOG.info("Create live node {}{}", zkClient.getConnectString(), liveNode);

    JsonObject content = new JsonObject();
    content.add("data", liveNodeData.get());
    return ZKOperations.ignoreError(zkClient.create(liveNode, encodeJson(content), CreateMode.EPHEMERAL),
                                    KeeperException.NodeExistsException.class, liveNode);
  }

  private OperationFuture<String> removeLiveNode() {
    String liveNode = getLiveNodePath();
    LOG.info("Remove live node {}{}", zkClient.getConnectString(), liveNode);
    return ZKOperations.ignoreError(zkClient.delete(liveNode), KeeperException.NoNodeException.class, liveNode);
  }

  private OperationFuture<String> removeServiceNode() {
    String serviceNode = String.format("/%s", id.getId());
    LOG.info("Remove service node {}{}", zkClient.getConnectString(), serviceNode);
    return ZKOperations.recursiveDelete(zkClient, serviceNode);
  }

  private void watchMessages() {
    final String messagesPath = getZKPath("messages");
    Futures.addCallback(zkClient.getChildren(messagesPath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        // TODO: Do we need to deal with other type of events?
        if (event.getType() == Event.EventType.NodeChildrenChanged && decoratedService.isRunning()) {
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
    });
  }

  private void processMessage(final String path, final String messageId) {
    Futures.addCallback(zkClient.getData(path), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        Message message = MessageCodec.decode(result.getData());
        if (message == null) {
          LOG.error("Failed to decode message for " + messageId + " in " + path);
          listenFailure(zkClient.delete(path, result.getStat().getVersion()));
          return;
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Message received from " + path + ": " + new String(MessageCodec.encode(message), Charsets.UTF_8));
        }
        if (handleStopMessage(message, getDeleteSupplier(path, result.getStat().getVersion()))) {
          return;
        }
        messageCallback.onReceived(callbackExecutor, path, result.getStat().getVersion(), messageId, message);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to fetch message content.", t);
      }
    });
  }

  private <V> boolean handleStopMessage(Message message, final Supplier<OperationFuture<V>> postHandleSupplier) {
    if (message.getType() == Message.Type.SYSTEM && SystemMessages.STOP_COMMAND.equals(message.getCommand())) {
      callbackExecutor.execute(new Runnable() {
        @Override
        public void run() {
          decoratedService.stop().addListener(new Runnable() {

            @Override
            public void run() {
              stopServiceOnComplete(postHandleSupplier.get(), ZKServiceDecorator.this);
            }
          }, MoreExecutors.sameThreadExecutor());
        }
      });
      return true;
    }
    return false;
  }


  private Supplier<OperationFuture<String>> getDeleteSupplier(final String path, final int version) {
    return new Supplier<OperationFuture<String>>() {
      @Override
      public OperationFuture<String> get() {
        return zkClient.delete(path, version);
      }
    };
  }

  private Listener createListener() {
    return new DecoratedServiceListener();
  }

  private <V> byte[] encode(V data, Class<? extends V> clz) {
    return new GsonBuilder().registerTypeAdapter(StateNode.class, new StateNodeCodec())
                            .registerTypeAdapter(StackTraceElement.class, new StackTraceElementCodec())
                            .create()
      .toJson(data, clz).getBytes(Charsets.UTF_8);
  }

  private byte[] encodeStateNode(StateNode stateNode) {
    return encode(stateNode, StateNode.class);
  }

  private <V extends JsonElement> byte[] encodeJson(V json) {
    return new Gson().toJson(json).getBytes(Charsets.UTF_8);
  }

  private String getZKPath(String path) {
    return String.format("/%s/%s", id, path);
  }

  private String getLiveNodePath() {
    return "/instances/" + id;
  }

  private static <V> OperationFuture<V> listenFailure(final OperationFuture<V> operationFuture) {
    operationFuture.addListener(new Runnable() {

      @Override
      public void run() {
        try {
          if (!operationFuture.isCancelled()) {
            operationFuture.get();
          }
        } catch (Exception e) {
          // TODO: what could be done besides just logging?
          LOG.error("Operation execution failed for " + operationFuture.getRequestPath(), e);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
    return operationFuture;
  }

  private static final class MessageCallbackCaller {
    private final MessageCallback callback;
    private final ZKClient zkClient;

    private MessageCallbackCaller(ZKClient zkClient) {
      this(null, zkClient);
    }

    private MessageCallbackCaller(MessageCallback callback, ZKClient zkClient) {
      this.callback = callback;
      this.zkClient = zkClient;
    }

    public void onReceived(Executor executor, final String path,
                           final int version, final String id, final Message message) {
      if (callback == null) {
        // Simply delete the message
        if (LOG.isDebugEnabled()) {
          LOG.debug("Ignoring incoming message from " + path + ": " + message);
        }
        listenFailure(zkClient.delete(path, version));
        return;
      }

      executor.execute(new Runnable() {
        @Override
        public void run() {
          try {
            // Message process is synchronous for now. Making it async needs more thoughts about race conditions.
            // The executor is the callbackExecutor which is a single thread executor.
            callback.onReceived(id, message).get();
          } catch (Throwable t) {
            LOG.error("Exception when processing message: {}, {}, {}", id, message, path, t);
          } finally {
            listenFailure(zkClient.delete(path, version));
          }
        }
      });
    }
  }

  private final class DecoratedServiceListener implements Listener {
    private volatile boolean zkFailure = false;

    @Override
    public void starting() {
      LOG.info("Starting: " + id);
      saveState(ServiceController.State.STARTING);
    }

    @Override
    public void running() {
      LOG.info("Running: " + id);
      notifyStarted();
      watchMessages();
      saveState(ServiceController.State.RUNNING);
    }

    @Override
    public void stopping(State from) {
      LOG.info("Stopping: " + id);
      saveState(ServiceController.State.STOPPING);
    }

    @Override
    public void terminated(State from) {
      LOG.info("Terminated: " + from + " " + id);
      if (zkFailure) {
        return;
      }

      ImmutableList<OperationFuture<String>> futures = ImmutableList.of(removeLiveNode(), removeServiceNode());
      final ListenableFuture<List<String>> future = Futures.allAsList(futures);
      Futures.successfulAsList(futures).addListener(new Runnable() {
        @Override
        public void run() {
          try {
            future.get();
            LOG.info("Service and state node removed");
            notifyStopped();
          } catch (Exception e) {
            LOG.warn("Failed to remove ZK nodes.", e);
            notifyFailed(e);
          }
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    }

    @Override
    public void failed(State from, final Throwable failure) {
      LOG.info("Failed: {} {}.", from, id, failure);
      if (zkFailure) {
        return;
      }

      ImmutableList<OperationFuture<String>> futures = ImmutableList.of(removeLiveNode(), removeServiceNode());
      Futures.successfulAsList(futures).addListener(new Runnable() {
        @Override
        public void run() {
          LOG.info("Service and state node removed");
          notifyFailed(failure);
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    }

    private void saveState(ServiceController.State state) {
      if (zkFailure) {
        return;
      }
      StateNode stateNode = new StateNode(state);
      stopOnFailure(zkClient.setData(getZKPath("state"), encodeStateNode(stateNode)));
    }

    private <V> void stopOnFailure(final OperationFuture<V> future) {
      future.addListener(new Runnable() {
        @Override
        public void run() {
          try {
            future.get();
          } catch (final Exception e) {
            LOG.error("ZK operation failed", e);
            zkFailure = true;
            decoratedService.stop().addListener(new Runnable() {
              @Override
              public void run() {
                notifyFailed(e);
              }
            }, Threads.SAME_THREAD_EXECUTOR);
          }
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    }
  }

  private <V> ListenableFuture<State> stopServiceOnComplete(ListenableFuture <V> future, final Service service) {
    return Futures.transform(future, new AsyncFunction<V, State>() {
      @Override
      public ListenableFuture<State> apply(V input) throws Exception {
        return service.stop();
      }
    });
  }
}
