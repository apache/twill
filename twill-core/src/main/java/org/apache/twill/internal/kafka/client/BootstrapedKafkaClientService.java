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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AbstractIdleService;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClientService;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by SFilippov on 11.07.2017.
 */
public class BootstrapedKafkaClientService extends AbstractIdleService implements KafkaClientService {
  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(BootstrapedKafkaClientService.class);

  private static final long CONSUMER_EXPIRE_MINUTES = 1L;   // close consumer if not used for 1 minute.

  private final String bootstrapServers;
  private final LinkedBlockingQueue<Cancellable> consumerCancels;
  private LoadingCache<Properties, KafkaConsumer<ByteBuffer, Integer>> consumers;
  private KafkaProducer<ByteBuffer, Integer> producer;

  public BootstrapedKafkaClientService(String bootstrapServers) {
    Preconditions.checkNotNull(bootstrapServers, "Bootstrap server's list cannot be null");

    Properties commonProps = new Properties();
    commonProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    commonProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, "simple-kafka-client");

    this.bootstrapServers = bootstrapServers;
    this.consumerCancels = new LinkedBlockingQueue<Cancellable>();
  }

  @Override
  protected void startUp() throws Exception {
    this.consumers = CacheBuilder.newBuilder()
      .expireAfterAccess(CONSUMER_EXPIRE_MINUTES, TimeUnit.MINUTES)
      .removalListener(createRemovalListener())
      .build(createConsumerLoader());
  }

  /**
   * Creates a CacheLoader for creating SimpleConsumer.
   */
  private CacheLoader<Properties, KafkaConsumer<ByteBuffer, Integer>> createConsumerLoader() {
    return new CacheLoader<Properties, KafkaConsumer<ByteBuffer, Integer>>() {
      @Override
      public KafkaConsumer<ByteBuffer, Integer> load(Properties properties) throws Exception {
        return new KafkaConsumer<ByteBuffer, Integer>(properties); //,"simple-kafka-client");
      }
    };
  }

  /**
   * Creates a RemovalListener that will close SimpleConsumer on cache removal.
   */
  private RemovalListener<Properties, KafkaConsumer<ByteBuffer, Integer>> createRemovalListener() {
    return new RemovalListener<Properties, KafkaConsumer<ByteBuffer, Integer>>() {
      @Override
      public void onRemoval(RemovalNotification<Properties, KafkaConsumer<ByteBuffer, Integer>> notification) {
        KafkaConsumer<ByteBuffer, Integer> consumer = notification.getValue();
        if (consumer != null) {
          consumer.close();
        }
      }
    };
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping Kafka consumer");
    List<Cancellable> cancels = Lists.newLinkedList();
    consumerCancels.drainTo(cancels);
    for (Cancellable cancel : cancels) {
      cancel.cancel();
    }
    consumers.invalidateAll();
    LOG.info("Kafka Consumer stopped");
  }

  @Override
  public KafkaPublisher getPublisher(KafkaPublisher.Ack ack, Compression compression) {
    return null;
  }

  @Override
  public org.apache.twill.kafka.client.KafkaConsumer getConsumer() {
    return new org.apache.twill.kafka.client.KafkaConsumer() {

      @Override
      public Preparer prepare() {
        return new Preparer() {
          @Override
          public Preparer add(String topic, int partition, long offset) {
            return null;
          }

          @Override
          public Preparer addFromBeginning(String topic, int partition) {
            return null;
          }

          @Override
          public Preparer addLatest(String topic, int partition) {
            return null;
          }

          @Override
          public Cancellable consume(MessageCallback callback) {
            return null;
          }
        };
      }
    };
  }
}