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
package org.apache.twill.internal.kafka.client;

import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;

/**
 * A Service to cache kafka broker information by subscribing to ZooKeeper.
 */
final class KafkaBrokerCache extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaBrokerCache.class);

  private static final String BROKERS_PATH = "/brokers";

  private final ZKClient zkClient;
  private final Map<String, InetSocketAddress> brokers;
  // topicBrokers is from topic->partition size->brokerId
  private final Map<String, SortedMap<Integer, Set<String>>> topicBrokers;
  private final Runnable invokeGetBrokers = new Runnable() {
    @Override
    public void run() {
      getBrokers();
    }
  };
  private final Runnable invokeGetTopics = new Runnable() {
    @Override
    public void run() {
      getTopics();
    }
  };

  KafkaBrokerCache(ZKClient zkClient) {
    this.zkClient = zkClient;
    this.brokers = Maps.newConcurrentMap();
    this.topicBrokers = Maps.newConcurrentMap();
  }

  @Override
  protected void startUp() throws Exception {
    getBrokers();
    getTopics();
  }

  @Override
  protected void shutDown() throws Exception {
    // No-op
  }

  public int getPartitionSize(String topic) {
    SortedMap<Integer, Set<String>> partitionBrokers = topicBrokers.get(topic);
    if (partitionBrokers == null || partitionBrokers.isEmpty()) {
      return 1;
    }
    return partitionBrokers.lastKey();
  }

  public TopicBroker getBrokerAddress(String topic, int partition) {
    SortedMap<Integer, Set<String>> partitionBrokers = topicBrokers.get(topic);
    if (partitionBrokers == null || partitionBrokers.isEmpty()) {
      return pickRandomBroker(topic);
    }

    // If the requested partition is greater than supported partition size, randomly pick one
    if (partition >= partitionBrokers.lastKey()) {
      return pickRandomBroker(topic);
    }

    // Randomly pick a partition size and randomly pick a broker from it
    Random random = new Random();
    partitionBrokers = partitionBrokers.tailMap(partition + 1);
    List<Integer> sizes = Lists.newArrayList(partitionBrokers.keySet());
    Integer partitionSize = pickRandomItem(sizes, random);
    List<String> ids = Lists.newArrayList(partitionBrokers.get(partitionSize));
    InetSocketAddress address = brokers.get(ids.get(new Random().nextInt(ids.size())));
    return address == null ? pickRandomBroker(topic) : new TopicBroker(topic, address, partitionSize);
  }

  private TopicBroker pickRandomBroker(String topic) {
    Map.Entry<String, InetSocketAddress> entry = Iterables.getFirst(brokers.entrySet(), null);
    if (entry == null) {
      return null;
    }
    InetSocketAddress address = entry.getValue();
    return new TopicBroker(topic, address, 0);
  }

  private <T> T pickRandomItem(List<T> list, Random random) {
    return list.get(random.nextInt(list.size()));
  }

  private void getBrokers() {
    final String idsPath = BROKERS_PATH + "/ids";

    Futures.addCallback(zkClient.getChildren(idsPath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        getBrokers();
      }
    }), new ExistsOnFailureFutureCallback<NodeChildren>(idsPath, invokeGetBrokers) {
      @Override
      public void onSuccess(NodeChildren result) {
        Set<String> children = ImmutableSet.copyOf(result.getChildren());
        for (String child : children) {
          getBrokenData(idsPath + "/" + child, child);
        }
        // Remove all removed brokers
        removeDiff(children, brokers);
      }
    });
  }

  private void getTopics() {
    final String topicsPath = BROKERS_PATH + "/topics";
    Futures.addCallback(zkClient.getChildren(topicsPath, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        getTopics();
      }
    }), new ExistsOnFailureFutureCallback<NodeChildren>(topicsPath, invokeGetTopics) {
      @Override
      public void onSuccess(NodeChildren result) {
        Set<String> children = ImmutableSet.copyOf(result.getChildren());

        // Process new children
        for (String topic : ImmutableSet.copyOf(Sets.difference(children, topicBrokers.keySet()))) {
          getTopic(topicsPath + "/" + topic, topic);
        }

        // Remove old children
        removeDiff(children, topicBrokers);
      }
    });
  }

  private void getBrokenData(String path, final String brokerId) {
    Futures.addCallback(zkClient.getData(path), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        String data = new String(result.getData(), Charsets.UTF_8);
        String hostPort = data.substring(data.indexOf(':') + 1);
        int idx = hostPort.indexOf(':');
        brokers.put(brokerId, new InetSocketAddress(hostPort.substring(0, idx),
                                                    Integer.parseInt(hostPort.substring(idx + 1))));
      }

      @Override
      public void onFailure(Throwable t) {
        // No-op, the watch on the parent node will handle it.
      }
    });
  }

  private void getTopic(final String path, final String topic) {
    Futures.addCallback(zkClient.getChildren(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        // Other event type changes are either could be ignored or handled by parent watcher
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
          getTopic(path, topic);
        }
      }
    }), new FutureCallback<NodeChildren>() {
      @Override
      public void onSuccess(NodeChildren result) {
        List<String> children = result.getChildren();
        final List<ListenableFuture<BrokerPartition>> futures = Lists.newArrayListWithCapacity(children.size());

        // Fetch data from each broken node
        for (final String brokerId : children) {
          Futures.transform(zkClient.getData(path + "/" + brokerId), new Function<NodeData, BrokerPartition>() {
            @Override
            public BrokerPartition apply(NodeData input) {
              return new BrokerPartition(brokerId, Integer.parseInt(new String(input.getData(), Charsets.UTF_8)));
            }
          });
        }

        // When all fetching is done, build the partition size->broker map for this topic
        Futures.successfulAsList(futures).addListener(new Runnable() {
          @Override
          public void run() {
            Map<Integer, Set<String>> partitionBrokers = Maps.newHashMap();
            for (ListenableFuture<BrokerPartition> future : futures) {
              try {
                BrokerPartition info = future.get();
                Set<String> brokerSet = partitionBrokers.get(info.getPartitionSize());
                if (brokerSet == null) {
                  brokerSet = Sets.newHashSet();
                  partitionBrokers.put(info.getPartitionSize(), brokerSet);
                }
                brokerSet.add(info.getBrokerId());
              } catch (Exception e) {
                // Exception is ignored, as it will be handled by parent watcher
              }
            }
            topicBrokers.put(topic, ImmutableSortedMap.copyOf(partitionBrokers));
          }
        }, Threads.SAME_THREAD_EXECUTOR);
      }

      @Override
      public void onFailure(Throwable t) {
        // No-op. Failure would be handled by parent watcher already (e.g. node not exists -> children change in parent)
      }
    });
  }

  private <K, V> void removeDiff(Set<K> keys, Map<K, V> map) {
    for (K key : ImmutableSet.copyOf(Sets.difference(map.keySet(), keys))) {
      map.remove(key);
    }
  }

  private abstract class ExistsOnFailureFutureCallback<V> implements FutureCallback<V> {

    private final String path;
    private final Runnable action;

    protected ExistsOnFailureFutureCallback(String path, Runnable action) {
      this.path = path;
      this.action = action;
    }

    @Override
    public final void onFailure(Throwable t) {
      if (!isNotExists(t)) {
        LOG.error("Fail to watch for kafka brokers: " + path, t);
        return;
      }

      waitExists(path);
    }

    private boolean isNotExists(Throwable t) {
      return ((t instanceof KeeperException) && ((KeeperException) t).code() == KeeperException.Code.NONODE);
    }

    private void waitExists(String path) {
      LOG.info("Path " + path + " not exists. Watch for creation.");

      // If the node doesn't exists, use the "exists" call to watch for node creation.
      Futures.addCallback(zkClient.exists(path, new Watcher() {
        @Override
        public void process(WatchedEvent event) {
          if (event.getType() == Event.EventType.NodeCreated || event.getType() == Event.EventType.NodeDeleted) {
            action.run();
          }
        }
      }), new FutureCallback<Stat>() {
        @Override
        public void onSuccess(Stat result) {
          // If path exists, get children again, otherwise wait for watch to get triggered
          if (result != null) {
            action.run();
          }
        }
        @Override
        public void onFailure(Throwable t) {
          action.run();
        }
      });
    }
  }

  private static final class BrokerPartition {
    private final String brokerId;
    private final int partitionSize;

    private BrokerPartition(String brokerId, int partitionSize) {
      this.brokerId = brokerId;
      this.partitionSize = partitionSize;
    }

    public String getBrokerId() {
      return brokerId;
    }

    public int getPartitionSize() {
      return partitionSize;
    }
  }
}
