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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.apache.twill.zookeeper.ZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A KafkaClientService that uses ZooKeeper for broker discovery.
 */
public class ZKKafkaClientService extends AbstractIdleService implements KafkaClientService, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ZKKafkaClientService.class);
  private static final long PUBLISHER_CLEANUP_SECONDS = 1;

  private final BrokerService brokerService;

  // Maintains a weak reference key map for calling publisher.shutdown when garbage collected.
  private final Map<WeakReference<KafkaPublisher>, Cancellable> publishers;
  private final ReferenceQueue<KafkaPublisher> referenceQueue;

  private final SimpleKafkaConsumer consumer;

  // For running cleanup job
  private ScheduledExecutorService scheduler;

  public ZKKafkaClientService(ZKClient zkClient) {
    this.brokerService = new ZKBrokerService(zkClient);
    this.publishers = Collections.synchronizedMap(new IdentityHashMap<WeakReference<KafkaPublisher>, Cancellable>());
    this.referenceQueue = new ReferenceQueue<KafkaPublisher>();
    this.consumer = new SimpleKafkaConsumer(brokerService);
  }

  @Override
  public KafkaPublisher getPublisher(KafkaPublisher.Ack ack, Compression compression) {
    Preconditions.checkState(isRunning(), "Service is not running.");

    // Wrap the publisher with a weak reference and save the cancellable for closing the publisher.
    SimpleKafkaPublisher publisher = new SimpleKafkaPublisher(brokerService, ack, compression);
    publishers.put(new WeakReference<KafkaPublisher>(publisher, referenceQueue), publisher.start());
    return publisher;
  }

  @Override
  public KafkaConsumer getConsumer() {
    Preconditions.checkState(isRunning(), "Service is not running.");
    return consumer;
  }

  @Override
  public void run() {
    // For calling publisher.producer.close() on garbage collected
    Reference<? extends KafkaPublisher> ref = referenceQueue.poll();
    while (ref != null && isRunning()) {
      publishers.remove(ref).cancel();
      ref = referenceQueue.poll();
    }
  }

  @Override
  protected void startUp() throws Exception {
    scheduler = Executors.newSingleThreadScheduledExecutor(Threads.createDaemonThreadFactory("kafka-client-cleanup"));
    scheduler.scheduleAtFixedRate(this, PUBLISHER_CLEANUP_SECONDS, PUBLISHER_CLEANUP_SECONDS, TimeUnit.SECONDS);

    // Start broker service to get auto-updated brokers information.
    brokerService.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping KafkaClientService");
    scheduler.shutdownNow();
    for (Cancellable cancellable : publishers.values()) {
      cancellable.cancel();
    }
    consumer.stop();

    brokerService.stopAndWait();
    LOG.info("KafkaClientService stopped");
  }
}
