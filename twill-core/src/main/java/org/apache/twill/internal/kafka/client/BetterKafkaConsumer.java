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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * {@link KafkaConsumer} implementation with new kafka api and bootstrap servers
 * Created by SFilippov on 18.07.2017.
 */
public class BetterKafkaConsumer implements KafkaConsumer {

  private static final int POOL_DELAY = 100;
  private static final int POOL_TIMEOUT = 100;

  private final ScheduledExecutorService executorService;
  private final org.apache.kafka.clients.consumer.KafkaConsumer<Integer, ByteBuffer> kafkaConsumer;

  public BetterKafkaConsumer(String bootstrapServers) {
    this.executorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
                                                                        .setDaemon(true)
                                                                        .setNameFormat("kafka-consumer-%d")
                                                                        .build());
    Properties properties = new Properties();
    properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "twill-log-consumer-group");
    properties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "twill-log-consumer-client-" + UUID.randomUUID());
    kafkaConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(properties,
                                                                          new IntegerDeserializer(),
                                                                          new ByteBufferDeserializer());
  }

  public void stop() throws InterruptedException {
    executorService.shutdown();
    if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
      executorService.shutdownNow();
    }
  }

  @Override
  public Preparer prepare() {
    return new BetterPreparer();
  }

  private final class BetterPreparer implements Preparer {

    private TopicPartition topicPartition;

    /**
     * Should be invoked only on {@link BetterKafkaConsumer#executorService}.
     *
     * @param topic topic to check if consumer subscribed
     */
    private void checkSubscription(String topic) {
      if (!kafkaConsumer.subscription().contains(topic)) {
        kafkaConsumer.subscribe(Collections.singleton(topic));
        kafkaConsumer.poll(0);
      }
    }

    @Override
    public Preparer add(final String topic, final int partition, final long offset) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          checkSubscription(topic);
          topicPartition = new TopicPartition(topic, partition);
          kafkaConsumer.seek(topicPartition, offset);
        }
      });
      return this;
    }

    @Override
    public Preparer addFromBeginning(final String topic, final int partition) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          checkSubscription(topic);
          topicPartition = new TopicPartition(topic, partition);
          kafkaConsumer.seekToBeginning(Collections.singleton(topicPartition));
        }
      });
      return this;
    }

    @Override
    public Preparer addLatest(final String topic, final int partition) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          checkSubscription(topic);
          topicPartition = new TopicPartition(topic, partition);
          kafkaConsumer.seekToEnd(Collections.singleton(topicPartition));
        }
      });
      return this;
    }

    @Override
    public Cancellable consume(final MessageCallback callback) {
      final ScheduledFuture<?> future = executorService.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          ConsumerRecords<Integer, ByteBuffer> records = kafkaConsumer.poll(POOL_TIMEOUT);
          Iterable<FetchedMessage> fetchedMessages =
            Iterables.transform(records, new Function<ConsumerRecord<Integer, ByteBuffer>, FetchedMessage>() {
              @Nullable
              @Override
              public FetchedMessage apply(@Nullable ConsumerRecord<Integer, ByteBuffer> record) {
                BasicFetchedMessage fetchedMessage =
                  new BasicFetchedMessage(new org.apache.twill.kafka.client.TopicPartition(record.topic(),
                                                                                           record.partition()));
                fetchedMessage.setPayload(record.value());
                fetchedMessage.setOffset(record.offset());
                fetchedMessage.setNextOffset(record.offset() + 1);
                return fetchedMessage;
              }
            });
          long offset = callback.onReceived(fetchedMessages.iterator());
          if (offset > 0) kafkaConsumer.seek(topicPartition, offset);
        }
      }, 0, POOL_DELAY, TimeUnit.MILLISECONDS);
      return new Cancellable() {
        @Override
        public void cancel() {
          future.cancel(true);
          callback.finished();
        }
      };
    }
  }
}
