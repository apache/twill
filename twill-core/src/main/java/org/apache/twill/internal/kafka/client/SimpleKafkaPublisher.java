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

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link KafkaPublisher} using the kafka scala-java api.
 */
final class SimpleKafkaPublisher implements KafkaPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaPublisher.class);

  private final BrokerService brokerService;
  private final Ack ack;
  private final Compression compression;
  private final AtomicReference<Producer<Integer, ByteBuffer>> producer;
  private final AtomicBoolean listenerCancelled;

  public SimpleKafkaPublisher(BrokerService brokerService, Ack ack, Compression compression) {
    this.brokerService = brokerService;
    this.ack = ack;
    this.compression = compression;
    this.producer = new AtomicReference<Producer<Integer, ByteBuffer>>();
    this.listenerCancelled = new AtomicBoolean(false);
  }

  /**
   * Start the publisher. This method must be called before other methods. This method is only to be called
   * by KafkaClientService who own this object.
   * @return A Cancellable for closing this publish.
   */
  Cancellable start() {
    ExecutorService listenerExecutor
      = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("kafka-publisher"));

    // Listen to changes in broker list
    final BrokerListChangeListener listener = new BrokerListChangeListener(listenerCancelled, producer,
                                                                           ack, compression);
    Cancellable cancelChangeListener = brokerService.addChangeListener(listener, listenerExecutor);

    // Invoke the change listener at least once. Since every call to the listener is through the single thread
    // executor, there is no race and for sure the listener always see the latest change, either through this call
    // or from the BrokerService callback.
    Future<?> completion = listenerExecutor.submit(new Runnable() {
      @Override
      public void run() {
        listener.changed(brokerService);
      }
    });

    Futures.getUnchecked(completion);
    return new ProducerCancellable(listenerExecutor, listenerCancelled, cancelChangeListener, producer);
  }

  @Override
  public Preparer prepare(String topic) {
    return new SimplePreparer(topic);
  }

  private final class SimplePreparer implements Preparer {

    private final String topic;
    private final List<KeyedMessage<Integer, ByteBuffer>> messages;

    private SimplePreparer(String topic) {
      this.topic = topic;
      this.messages = Lists.newLinkedList();
    }

    @Override
    public Preparer add(ByteBuffer message, Object partitionKey) {
      messages.add(new KeyedMessage<Integer, ByteBuffer>(topic, Math.abs(partitionKey.hashCode()), message));
      return this;
    }

    @Override
    public ListenableFuture<Integer> send() {
      try {
        int size = messages.size();
        Producer<Integer, ByteBuffer> kafkaProducer = producer.get();
        if (kafkaProducer == null) {
          return Futures.immediateFailedFuture(new IllegalStateException("No kafka producer available."));
        }
        kafkaProducer.send(messages);
        return Futures.immediateFuture(size);
      } catch (Exception e) {
        return Futures.immediateFailedFuture(e);
      } finally {
        messages.clear();
      }
    }
  }

  /**
   * Listener for watching for changes in broker list.
   * This needs to be a static class so that no reference to the publisher instance is held in order for
   * weak reference inside ZKBrokerService to publish works and be able to GC the Publisher instance and hence
   * closing the underlying kafka producer.
   */
  private static final class BrokerListChangeListener extends BrokerService.BrokerChangeListener {

    private final AtomicBoolean listenerCancelled;
    private final AtomicReference<Producer<Integer, ByteBuffer>> producer;
    private final Ack ack;
    private final Compression compression;
    private String brokerList;

    private BrokerListChangeListener(AtomicBoolean listenerCancelled,
                                     AtomicReference<Producer<Integer, ByteBuffer>> producer,
                                     Ack ack, Compression compression) {
      this.listenerCancelled = listenerCancelled;
      this.producer = producer;
      this.ack = ack;
      this.compression = compression;
    }

    @Override
    public void changed(BrokerService brokerService) {
      if (listenerCancelled.get()) {
        return;
      }

      String newBrokerList = brokerService.getBrokerList();
      if (newBrokerList.isEmpty()) {
        LOG.warn("Broker list is empty. No Kafka producer is created.");
        return;
      }

      if (Objects.equal(brokerList, newBrokerList)) {
        return;
      }

      Properties props = new Properties();
      props.put("metadata.broker.list", newBrokerList);
      props.put("serializer.class", ByteBufferEncoder.class.getName());
      props.put("key.serializer.class", IntegerEncoder.class.getName());
      props.put("partitioner.class", IntegerPartitioner.class.getName());
      props.put("request.required.acks", Integer.toString(ack.getAck()));
      props.put("compression.codec", compression.getCodec());

      ProducerConfig config = new ProducerConfig(props);
      Producer<Integer, ByteBuffer> oldProducer = producer.getAndSet(new Producer<Integer, ByteBuffer>(config));
      if (oldProducer != null) {
        oldProducer.close();
      }

      LOG.info("Update Kafka producer broker list: {}", newBrokerList);
      brokerList = newBrokerList;
    }
  }

  /**
   * For stopping and releasing resource for the publisher. This class shouldn't hold any references to the
   * Publisher class.
   */
  private static final class ProducerCancellable implements Cancellable, Runnable {
    private final ExecutorService executor;
    private final AtomicBoolean listenerCancelled;
    private final Cancellable cancelChangeListener;
    private final AtomicReference<Producer<Integer, ByteBuffer>> producer;

    private ProducerCancellable(ExecutorService executor, AtomicBoolean listenerCancelled,
                                Cancellable cancelChangeListener,
                                AtomicReference<Producer<Integer, ByteBuffer>> producer) {
      this.executor = executor;
      this.listenerCancelled = listenerCancelled;
      this.cancelChangeListener = cancelChangeListener;
      this.producer = producer;
    }

    @Override
    public void cancel() {
      if (listenerCancelled.compareAndSet(false, true)) {
        executor.execute(this);
      }
    }

    @Override
    public void run() {
      // Call from cancel() through executor only.
      cancelChangeListener.cancel();
      Producer<Integer, ByteBuffer> kafkaProducer = producer.get();
      kafkaProducer.close();
      executor.shutdownNow();
    }
  }
}
