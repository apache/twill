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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by SFilippov on 11.07.2017.
 */
public class BootstrapedKafkaClientService extends AbstractIdleService implements KafkaClientService {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(BootstrapedKafkaClientService.class);

  private static final long CONSUMER_EXPIRE_MINUTES = 1L;   // close consumer if not used for 1 minute.

  private final String bootstrapServers;
  private final List<BetterKafkaPublisher> publishers;
  private final BetterKafkaConsumer consumer;

  public BootstrapedKafkaClientService(String bootstrapServers) {
    Preconditions.checkNotNull(bootstrapServers, "Bootstrap server's list cannot be null");
    this.bootstrapServers = bootstrapServers;
    consumer = new BetterKafkaConsumer(bootstrapServers);
    publishers = Lists.newArrayList();
  }

  @Override
  protected void startUp() throws Exception {
//    this.consumers = CacheBuilder.newBuilder()
//      .expireAfterAccess(CONSUMER_EXPIRE_MINUTES, TimeUnit.MINUTES)
//      .removalListener(createRemovalListener())
//      .build(createConsumerLoader());
  }

//  /**
//   * Creates a CacheLoader for creating SimpleConsumer.
//   */
//  private CacheLoader<Properties, KafkaConsumer<String, String>> createConsumerLoader() {
//    return new CacheLoader<Properties, KafkaConsumer<String, String>>() {
//      @Override
//      public KafkaConsumer<String, String> load(Properties properties) throws Exception {
//        return new KafkaConsumer<>(properties);
//      }
//    };
//  }
//
//  /**
//   * Creates a RemovalListener that will close SimpleConsumer on cache removal.
//   */
//  private RemovalListener<Properties, KafkaConsumer<String, String>> createRemovalListener() {
//    return new RemovalListener<Properties, KafkaConsumer<String, String>>() {
//      @Override
//      public void onRemoval(RemovalNotification<Properties, KafkaConsumer<String, String>> notification) {
//        KafkaConsumer<String, String> consumer = notification.getValue();
//        if (consumer != null) {
//          consumer.close();
//        }
//      }
//    };
//  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Kafka consumer");
    consumer.stop();
    for (BetterKafkaPublisher publisher : publishers) {
      publisher.stop();
    }
    LOG.info("Kafka Consumer stopped");
  }

  @Override
  public KafkaPublisher getPublisher(KafkaPublisher.Ack ack, Compression compression) {
    Preconditions.checkState(isRunning(), "Service is not running.");
    BetterKafkaPublisher publisher = new BetterKafkaPublisher(bootstrapServers, ack, compression);
    publishers.add(publisher);
    return publisher;
  }

  @Override
  public org.apache.twill.kafka.client.KafkaConsumer getConsumer() {
    Preconditions.checkState(isRunning(), "Service is not running.");
    return consumer;
  }
}
