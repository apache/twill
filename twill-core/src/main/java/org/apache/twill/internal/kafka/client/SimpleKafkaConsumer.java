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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.kafka.client.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link KafkaConsumer} implementation using the scala kafka api.
 */
final class SimpleKafkaConsumer implements KafkaConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaConsumer.class);
  private static final int FETCH_SIZE = 1024 * 1024;        // Use a default fetch size.
  private static final int SO_TIMEOUT = 5 * 1000;           // 5 seconds.
  private static final int MAX_WAIT = 1000;                 // 1 second.
  private static final long CONSUMER_EXPIRE_MINUTES = 1L;   // close consumer if not used for 1 minute.
  private static final long INIT_CONSUMER_FAILURE_BACKOFF = 100L; // Initial backoff for 100ms if failure in consumer.
  private static final long MAX_CONSUMER_FAILURE_BACKOFF = 10000L; // Backoff max for 10 seconds if failure in consumer.
  private static final long EMPTY_FETCH_WAIT = 500L;        // Sleep for 500 ms if no message is fetched.

  private final BrokerService brokerService;
  private final LoadingCache<BrokerInfo, SimpleConsumer> consumers;
  private final BlockingQueue<Cancellable> consumerCancels;

  SimpleKafkaConsumer(BrokerService brokerService) {
    this.brokerService = brokerService;
    this.consumers = CacheBuilder.newBuilder()
                                 .expireAfterAccess(CONSUMER_EXPIRE_MINUTES, TimeUnit.MINUTES)
                                 .removalListener(createRemovalListener())
                                 .build(createConsumerLoader());
    this.consumerCancels = new LinkedBlockingQueue<Cancellable>();
  }

  @Override
  public Preparer prepare() {
    return new SimplePreparer();
  }

  /**
   * Called to stop all consumers created. This method should only be
   * called by KafkaClientService who own this consumer.
   */
  void stop() {
    LOG.info("Stopping Kafka consumer");
    List<Cancellable> cancels = Lists.newLinkedList();
    consumerCancels.drainTo(cancels);
    for (Cancellable cancel : cancels) {
      cancel.cancel();
    }
    consumers.invalidateAll();
    LOG.info("Kafka Consumer stopped");
  }

  /**
   * Creates a CacheLoader for creating SimpleConsumer.
   */
  private CacheLoader<BrokerInfo, SimpleConsumer> createConsumerLoader() {
    return new CacheLoader<BrokerInfo, SimpleConsumer>() {
      @Override
      public SimpleConsumer load(BrokerInfo key) throws Exception {
        return new SimpleConsumer(key.getHost(), key.getPort(), SO_TIMEOUT, FETCH_SIZE, "simple-kafka-client");
      }
    };
  }

  /**
   * Creates a RemovalListener that will close SimpleConsumer on cache removal.
   */
  private RemovalListener<BrokerInfo, SimpleConsumer> createRemovalListener() {
    return new RemovalListener<BrokerInfo, SimpleConsumer>() {
      @Override
      public void onRemoval(RemovalNotification<BrokerInfo, SimpleConsumer> notification) {
        SimpleConsumer consumer = notification.getValue();
        if (consumer != null) {
          consumer.close();
        }
      }
    };
  }

  /**
   * Retrieves the last offset before the given timestamp for a given topic partition.
   *
   * @return The last offset before the given timestamp or {@code 0} if failed to do so.
   */
  private long getLastOffset(TopicPartition topicPart, long timestamp) {
    BrokerInfo brokerInfo = brokerService.getLeader(topicPart.getTopic(), topicPart.getPartition());
    SimpleConsumer consumer = brokerInfo == null ? null : consumers.getUnchecked(brokerInfo);

    // If no broker, treat it as failure attempt.
    if (consumer == null) {
      LOG.warn("Failed to talk to any broker. Default offset to 0 for {}", topicPart);
      return 0L;
    }

    // Fire offset request
    OffsetRequest request = new OffsetRequest(ImmutableMap.of(
      new TopicAndPartition(topicPart.getTopic(), topicPart.getPartition()),
      new PartitionOffsetRequestInfo(timestamp, 1)
    ), kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

    OffsetResponse response = consumer.getOffsetsBefore(request);

    // Retrieve offsets from response
    long[] offsets = response.hasError() ? null : response.offsets(topicPart.getTopic(), topicPart.getPartition());
    if (offsets == null || offsets.length <= 0) {
      short errorCode = response.errorCode(topicPart.getTopic(), topicPart.getPartition());

      // If the topic partition doesn't exists, use offset 0 without logging error.
      if (errorCode != ErrorMapping.UnknownTopicOrPartitionCode()) {
        consumers.refresh(brokerInfo);
        LOG.warn("Failed to fetch offset for {} with timestamp {}. Error: {}. Default offset to 0.",
                 topicPart, timestamp, errorCode);
      }
      return 0L;
    }

    LOG.debug("Offset {} fetched for {} with timestamp {}.", offsets[0], topicPart, timestamp);
    return offsets[0];
  }

  /**
   * A preparer that uses kafak scala api for consuming messages.
   */
  private final class SimplePreparer implements Preparer {

    // Map from TopicPartition to offset
    private final Map<TopicPartition, Long> requests;
    private final ThreadFactory threadFactory;

    private SimplePreparer() {
      this.requests = Maps.newHashMap();
      this.threadFactory = Threads.createDaemonThreadFactory("message-callback-%d");
    }

    @Override
    public Preparer add(String topic, int partition, long offset) {
      requests.put(new TopicPartition(topic, partition), offset);
      return this;
    }

    @Override
    public Preparer addFromBeginning(String topic, int partition) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      requests.put(topicPartition, kafka.api.OffsetRequest.EarliestTime());
      return this;
    }

    @Override
    public Preparer addLatest(String topic, int partition) {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      requests.put(topicPartition, kafka.api.OffsetRequest.LatestTime());
      return this;
    }

    @Override
    public Cancellable consume(MessageCallback callback) {
      final ExecutorService executor = Executors.newSingleThreadExecutor(threadFactory);
      final List<ConsumerThread> pollers = Lists.newArrayList();

      // When cancelling the consumption, first terminates all polling threads and then stop the executor service.
      final AtomicBoolean cancelled = new AtomicBoolean();
      Cancellable cancellable = new Cancellable() {
        @Override
        public void cancel() {
          if (!cancelled.compareAndSet(false, true)) {
            return;
          }
          consumerCancels.remove(this);

          LOG.info("Requesting stop of all consumer threads.");
          for (ConsumerThread consumerThread : pollers) {
            consumerThread.terminate();
          }
          LOG.info("Wait for all consumer threads to stop.");
          for (ConsumerThread consumerThread : pollers) {
            try {
              consumerThread.join();
            } catch (InterruptedException e) {
              LOG.warn("Interrupted exception while waiting for thread to complete.", e);
            }
          }
          LOG.info("All consumer threads stopped.");
          // Use shutdown so that submitted task still has chance to execute, which is important for finished to get
          // called.
          executor.shutdown();
        }
      };

      // Wrap the callback with a single thread executor.
      MessageCallback messageCallback = wrapCallback(callback, executor, cancellable);

      // Starts threads for polling new messages.
      for (Map.Entry<TopicPartition, Long> entry : requests.entrySet()) {
        ConsumerThread consumerThread = new ConsumerThread(entry.getKey(), entry.getValue(), messageCallback);
        consumerThread.setDaemon(true);
        consumerThread.start();
        pollers.add(consumerThread);
      }

      consumerCancels.add(cancellable);
      return cancellable;
    }

    /**
     * Wrap a given MessageCallback by a executor so that calls are executed in the given executor.
     * By running the calls through the executor, it also block and wait for the task being completed so that
     * it can block the poller thread depending on the rate of processing that the callback can handle.
     */
    private MessageCallback wrapCallback(final MessageCallback callback,
                                         final ExecutorService executor, final Cancellable cancellable) {
      final AtomicBoolean stopped = new AtomicBoolean();
      return new MessageCallback() {
        @Override
        public long onReceived(final Iterator<FetchedMessage> messages) {
          if (stopped.get()) {
            return -1L;
          }
          return Futures.getUnchecked(executor.submit(new Callable<Long>() {
            @Override
            public Long call() {
              if (stopped.get()) {
                return -1L;
              }
              return callback.onReceived(messages);
            }
          }));
        }

        @Override
        public void finished() {
          // Make sure finished only get called once.
          if (!stopped.compareAndSet(false, true)) {
            return;
          }
          Futures.getUnchecked(executor.submit(new Runnable() {
            @Override
            public void run() {
              // When finished is called, also cancel the consumption from all polling thread.
              callback.finished();
              cancellable.cancel();
            }
          }));
        }
      };
    }
  }

  /**
   * The thread for polling kafka.
   */
  private final class ConsumerThread extends Thread {

    private final TopicPartition topicPart;
    private final long startOffset;
    private final MessageCallback callback;
    private final BasicFetchedMessage fetchedMessage;
    private volatile boolean running;

    private ConsumerThread(TopicPartition topicPart, long startOffset, MessageCallback callback) {
      super(String.format("Kafka-Consumer-%s-%d", topicPart.getTopic(), topicPart.getPartition()));
      this.topicPart = topicPart;
      this.startOffset = startOffset;
      this.callback = callback;
      this.running = true;
      this.fetchedMessage = new BasicFetchedMessage(topicPart);
    }

    @Override
    public void run() {
      final AtomicLong offset = new AtomicLong(startOffset);

      Map.Entry<BrokerInfo, SimpleConsumer> consumerEntry = null;
      ExponentialBackoff backoff = new ExponentialBackoff(INIT_CONSUMER_FAILURE_BACKOFF,
                                                          MAX_CONSUMER_FAILURE_BACKOFF, TimeUnit.MILLISECONDS);
      while (running) {
        if (consumerEntry == null && (consumerEntry = getConsumerEntry()) == null) {
          LOG.debug("No leader for topic partition {}.", topicPart);
          backoff.backoff();
          continue;
        }

        // If offset < 0, meaning it's special offset value that needs to fetch either the earliest or latest offset
        // from kafak server.
        long off = offset.get();
        if (off < 0) {
          offset.set(getLastOffset(topicPart, off));
        }

        SimpleConsumer consumer = consumerEntry.getValue();

        // Fire a fetch message request
        try {
          FetchResponse response = fetchMessages(consumer, offset.get());

          // Failure response, set consumer entry to null and let next round of loop to handle it.
          if (response.hasError()) {
            short errorCode = response.errorCode(topicPart.getTopic(), topicPart.getPartition());
            LOG.info("Failed to fetch message on {}. Error: {}", topicPart, errorCode);
            // If it is out of range error, reset to earliest offset
            if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
              offset.set(kafka.api.OffsetRequest.EarliestTime());
            }

            consumers.refresh(consumerEntry.getKey());
            consumerEntry = null;
            continue;
          }

          ByteBufferMessageSet messages = response.messageSet(topicPart.getTopic(), topicPart.getPartition());
          if (sleepIfEmpty(messages)) {
            continue;
          }

          // Call the callback
          invokeCallback(messages, offset);
          backoff.reset();
        } catch (Throwable t) {
          // Only log if it is still running, otherwise, it just the interrupt caused by the stop.
          if (!running) {
            LOG.debug("Unable to fetch messages on {}, kafka consumer service shutdown is in progress.", topicPart);
          } else {
            if (t instanceof ClosedByInterruptException || t instanceof ConnectException) {
              LOG.debug("Unable to fetch messages on {}, kafka server shutdown is in progress.", topicPart);
            } else {
              LOG.info("Exception when fetching message on {}.", topicPart, t);
            }
            backoff.backoff();
          }
          consumers.refresh(consumerEntry.getKey());
          consumerEntry = null;
        }
      }

      // When the thread is done, call the callback finished method.
      try {
        callback.finished();
      } catch (Throwable t) {
        LOG.error("Exception thrown from MessageCallback.finished({})", running, t);
      }
    }

    public void terminate() {
      LOG.info("Terminate requested {}", getName());
      running = false;
      interrupt();
    }

    /**
     * Gets the leader broker and the associated SimpleConsumer for the current topic and partition.
     */
    private Map.Entry<BrokerInfo, SimpleConsumer> getConsumerEntry() {
      BrokerInfo leader = brokerService.getLeader(topicPart.getTopic(), topicPart.getPartition());
      return leader == null ? null : Maps.immutableEntry(leader, consumers.getUnchecked(leader));
    }

    /**
     * Makes a call to kafka to fetch messages.
     */
    private FetchResponse fetchMessages(SimpleConsumer consumer, long offset) {
      FetchRequest request = new FetchRequestBuilder()
        .clientId(consumer.clientId())
        .addFetch(topicPart.getTopic(), topicPart.getPartition(), offset, FETCH_SIZE)
        .maxWait(MAX_WAIT)
        .build();
      return consumer.fetch(request);
    }

    /**
     * Sleeps if the message set is empty.
     * @return {@code true} if it is empty, {@code false} otherwise.
     */
    private boolean sleepIfEmpty(ByteBufferMessageSet messages) {
      if (Iterables.isEmpty(messages)) {
        LOG.trace("No message fetched. Sleep for {} ms before next fetch.", EMPTY_FETCH_WAIT);
        try {
          TimeUnit.MILLISECONDS.sleep(EMPTY_FETCH_WAIT);
        } catch (InterruptedException e) {
          // It's interrupted from stop, ok to ignore.
        }
        return true;
      }
      return false;
    }

    /**
     * Calls the message callback with the given message set.
     */
    private void invokeCallback(ByteBufferMessageSet messages, AtomicLong offset) {
      long savedOffset = offset.get();
      try {
        offset.set(callback.onReceived(createFetchedMessages(messages, offset)));
      } catch (Throwable t) {
        LOG.error("Callback throws exception. Retry from offset {} for {}", startOffset, topicPart, t);
        offset.set(savedOffset);
      }
    }

    /**
     * Creates an Iterator of FetchedMessage based on the given message set. The iterator would also updates
     * the offset while iterating.
     */
    private Iterator<FetchedMessage> createFetchedMessages(ByteBufferMessageSet messageSet, final AtomicLong offset) {
      final Iterator<MessageAndOffset> messages = messageSet.iterator();
      return new AbstractIterator<FetchedMessage>() {
        @Override
        protected FetchedMessage computeNext() {
          while (messages.hasNext()) {
            MessageAndOffset message = messages.next();
            long msgOffset = message.offset();
            if (msgOffset < offset.get()) {
              LOG.trace("Received old offset {}, expecting {} on {}. Message Ignored.",
                        msgOffset, offset.get(), topicPart);
              continue;
            }

            fetchedMessage.setPayload(message.message().payload());
            fetchedMessage.setOffset(message.offset());
            fetchedMessage.setNextOffset(message.nextOffset());

            return fetchedMessage;
          }
          return endOfData();
        }
      };
    }

    /**
     * Helper class for performance exponential backoff on message fetching failure.
     */
    private final class ExponentialBackoff {
      private final long initialBackoff;
      private final long maxBackoff;
      private final TimeUnit backoffUnit;
      private int failureCount = 0;

      private ExponentialBackoff(long initialBackoff, long maxBackoff, TimeUnit backoffUnit) {
        this.initialBackoff = initialBackoff;
        this.maxBackoff = maxBackoff;
        this.backoffUnit = backoffUnit;
      }

      void backoff() {
        failureCount++;
        long multiplier = failureCount > Long.SIZE ? Long.MAX_VALUE : (1L << (failureCount - 1));
        long backoff = Math.min(initialBackoff * multiplier, maxBackoff);
        backoff = backoff < 0 ? maxBackoff : backoff;
        try {
          backoffUnit.sleep(backoff);
        } catch (InterruptedException e) {
          // OK to ignore since this method is called from the consumer thread only, which on thread shutdown,
          // the thread will be interrupted
        }
      }

      void reset() {
        failureCount = 0;
      }
    }
  }
}
